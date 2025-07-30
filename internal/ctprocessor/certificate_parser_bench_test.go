package ctprocessor

import (
	"crypto/x509"
	"encoding/base64"
	"testing"
)

func BenchmarkDecodeBase64Certificate(b *testing.B) {
	cp := NewCertificateParser()

	// Create test certificate data with whitespace
	testData := "Hello, World! This is a test certificate data that needs to be decoded"
	encodedData := base64.StdEncoding.EncodeToString([]byte(testData))
	// Add whitespace to simulate real-world data
	certData := " " + encodedData[:20] + " \t\n " + encodedData[20:40] + " \r\n " + encodedData[40:] + " \n "

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cp.DecodeBase64Certificate(certData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExtractDomainMappings(b *testing.B) {
	cp := NewCertificateParser()

	// Create a test certificate with multiple SANs
	cert := createTestCertificateWithNames(b, "example.com", []string{
		"api.example.com",
		"www.example.com",
		"test.example.com",
		"staging.example.com",
		"dev.example.com",
		"*.example.com",
		"mail.example.com",
		"admin.example.com",
	}, "Example Corporation Ltd")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cp.ExtractDomainMappings(cert)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExtractDomainMappingsNoOrg(b *testing.B) {
	cp := NewCertificateParser()

	// Create a test certificate without organization
	cert := createTestCertificateWithNames(b, "example.com", []string{
		"api.example.com",
		"www.example.com",
	}, "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cp.ExtractDomainMappings(cert)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExtractOrganization(b *testing.B) {
	cp := NewCertificateParser()

	// Create certificates with different organization scenarios
	certs := []*x509.Certificate{
		createTestCertificateWithNames(b, "example.com", []string{"www.example.com"}, "Example Corp"),
		createTestCertificateWithNames(b, "test.com", []string{"www.test.com"}, ""),
		createTestCertificateWithNames(b, "company.inc", []string{"www.company.inc"}, ""),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cert := certs[i%len(certs)]
		_, _ = cp.extractOrganization(cert)
	}
}

func BenchmarkProcessCertificateWithRecovery(b *testing.B) {
	cp := NewCertificateParser()

	// Create test certificate and encode it
	testCert := createTestCertificateWithNames(b, "example.com", []string{
		"api.example.com",
		"www.example.com",
		"test.example.com",
	}, "Example Corp")
	encodedCert := base64.StdEncoding.EncodeToString(testCert.Raw)

	entry := &CertificateEntry{
		SequenceNumber:  123,
		CertificateData: encodedCert,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset ParsedCert to simulate real processing
		entry.ParsedCert = nil
		_, err := cp.ProcessCertificateWithRecovery(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegistered(b *testing.B) {
	domains := []string{
		"example.com",
		"*.example.com",
		"api.test.example.com",
		"www.subdomain.example.co.uk",
		"測試.example.com", // Unicode domain
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		domain := domains[i%len(domains)]
		_, _, err := Registered(domain)
		if err != nil && i%len(domains) != 4 { // Unicode domain might error
			b.Fatal(err)
		}
	}
}
