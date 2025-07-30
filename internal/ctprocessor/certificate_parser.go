package ctprocessor

import (
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
)

// CertificateParser handles certificate decoding and parsing operations
type CertificateParser struct{}

// NewCertificateParser creates a new CertificateParser instance
func NewCertificateParser() *CertificateParser {
	return &CertificateParser{}
}

// DecodeBase64Certificate decodes base64-encoded certificate data
func (cp *CertificateParser) DecodeBase64Certificate(certData string) ([]byte, error) {
	if strings.TrimSpace(certData) == "" {
		return nil, fmt.Errorf("certificate data is empty")
	}

	decoded, err := base64.StdEncoding.DecodeString(certData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 certificate data: %v", err)
	}

	if len(decoded) == 0 {
		return nil, fmt.Errorf("decoded certificate data is empty")
	}

	return decoded, nil
}

// ParseX509Certificate parses DER-encoded certificate data into an X.509 certificate
func (cp *CertificateParser) ParseX509Certificate(derData []byte) (*x509.Certificate, error) {
	if len(derData) == 0 {
		return nil, fmt.Errorf("certificate DER data is empty")
	}

	cert, err := x509.ParseCertificate(derData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse X.509 certificate: %v", err)
	}

	return cert, nil
}

// ParseCertificateEntry processes a CertificateEntry by decoding and parsing the certificate
func (cp *CertificateParser) ParseCertificateEntry(entry *CertificateEntry) error {
	if entry == nil {
		return fmt.Errorf("certificate entry is nil")
	}

	if err := entry.Validate(); err != nil {
		return fmt.Errorf("invalid certificate entry: %v", err)
	}

	derData, err := cp.DecodeBase64Certificate(entry.CertificateData)
	if err != nil {
		return fmt.Errorf("failed to decode certificate for sequence %d: %v", entry.SequenceNumber, err)
	}

	cert, err := cp.ParseX509Certificate(derData)
	if err != nil {
		return fmt.Errorf("failed to parse certificate for sequence %d: %v", entry.SequenceNumber, err)
	}

	entry.ParsedCert = cert
	return nil
}

var ascii = idna.New(idna.MapForLookup(), idna.StrictDomainName(false))

func Registered(s string) (string, string, error) {
	d, err := ascii.ToASCII(strings.Trim(strings.TrimPrefix(strings.ToLower(s), "*."), "."))
	if err != nil {
		return "", "", err
	}
	effectiveTLDPlusOne, err := publicsuffix.EffectiveTLDPlusOne(d)
	return d, effectiveTLDPlusOne, err
}

// ExtractDomainMappings extracts domain mappings from a parsed X.509 certificate
func (cp *CertificateParser) ExtractDomainMappings(cert *x509.Certificate) ([]DomainMapping, error) {
	if cert == nil {
		return nil, fmt.Errorf("certificate is nil")
	}

	var mappings []DomainMapping

	organization, organizationalUnit := cp.extractOrganization(cert)

	if organization == "" || organization == "Domain Control Validated" {
		return mappings, nil
	}
	// Track normalized domains to detect self-referential certificates
	subdomainMap := make(map[string]struct{})

	errors := make([]error, 0)
	
	if cn := strings.TrimSpace(cert.Subject.CommonName); cn != "" {
		normalizedCn, domain, err := Registered(cn)
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to register domain from Common Name: %v", err))
		}
		subdomainMap[normalizedCn] = struct{}{}
		mapping := DomainMapping{
			Domain:             domain,
			Name:               cn,
			Organization:       organization,
			OrganizationalUnit: organizationalUnit,
			Source:             "CN",
			SequenceNum:        0, // Will be set by caller
		}
		if err := mapping.Validate(); err == nil {
			mappings = append(mappings, mapping)
		}
	}

	for _, san := range cert.DNSNames {
		normalizedcn, domain, err := Registered(san)
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to register domain from SAN '%s': %v", san, err))
		}
		subdomainMap[normalizedcn] = struct{}{}
		if san = strings.TrimSpace(san); san != "" {
			mapping := DomainMapping{
				Domain:             domain,
				Name:               san,
				Organization:       organization,
				OrganizationalUnit: organizationalUnit,
				Source:             "SAN",
				SequenceNum:        0, // Will be set by caller
			}
			if err := mapping.Validate(); err == nil {
				mappings = append(mappings, mapping)
			}
		}
	}
	if len(mappings) == 0 && len(errors) > 0 {
		return nil, fmt.Errorf("no valid domain mappings found in certificate: %v", errors)
	}

	if organization == strings.ToLower(organization) {
		normalizedOrganization, _, err := Registered(organization)
		if err != nil {
			return mappings, nil
		}
		// Skip certificates where organization name matches a subdomain (self-referential)
		if _, exists := subdomainMap[normalizedOrganization]; exists {
			return nil, nil
		}
	}

	return mappings, nil
}

// extractOrganization extracts the organization from the certificate subject
func (cp *CertificateParser) extractOrganization(cert *x509.Certificate) (string, string) {
	var org string
	var ou string
	if len(cert.Subject.Organization) > 0 {
		org = strings.TrimSpace(cert.Subject.Organization[0])
	}

	if len(cert.Subject.OrganizationalUnit) > 0 {
		ou = strings.TrimSpace(cert.Subject.OrganizationalUnit[0])
	}
	return org, ou
}

// ProcessCertificateWithRecovery processes a certificate entry with error recovery
func (cp *CertificateParser) ProcessCertificateWithRecovery(entry *CertificateEntry) ([]DomainMapping, error) {
	if err := cp.ParseCertificateEntry(entry); err != nil {
		return nil, err
	}

	mappings, err := cp.ExtractDomainMappings(entry.ParsedCert)
	if err != nil {
		return nil, fmt.Errorf("failed to extract mappings for sequence %d: %v", entry.SequenceNumber, err)
	}

	// Set sequence numbers for all mappings
	for i := range mappings {
		mappings[i].SequenceNum = entry.SequenceNumber
	}

	return mappings, nil
}

// ValidateCertificate performs basic validation on a parsed certificate
func (cp *CertificateParser) ValidateCertificate(cert *x509.Certificate) error {
	if cert == nil {
		return fmt.Errorf("certificate is nil")
	}

	// Check if certificate has expired (optional validation)
	// Note: We don't fail on expired certificates as they're still valid for our analysis

	// Check if certificate has basic required fields
	if cert.Subject.String() == "" {
		return fmt.Errorf("certificate has empty subject")
	}

	// Check if certificate has at least one name (CN or SAN)
	hasName := false
	if strings.TrimSpace(cert.Subject.CommonName) != "" {
		hasName = true
	}
	if len(cert.DNSNames) > 0 {
		for _, san := range cert.DNSNames {
			if strings.TrimSpace(san) != "" {
				hasName = true
				break
			}
		}
	}

	if !hasName {
		return fmt.Errorf("certificate has no valid names (CN or SAN)")
	}

	return nil
}
