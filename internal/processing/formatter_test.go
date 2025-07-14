package processing

import (
	"encoding/base64"
	"testing"
)

// This test uses the exact data you provided for the failing certificate 140052.
func TestExtractCertificateFromLeaf(t *testing.T) {
	// Test case for a standard X.509 certificate entry.
	// (Using a placeholder - a real test suite would have a valid example here)
	t.Run("handles standard x509 entry", func(t *testing.T) {
		// This leaf input would have a LogEntryType of 0.
		// For now, we are focusing on the failing case.
		t.Skip("skipping standard entry test for now")
	})

	// Test case for the precertificate entry that was failing.
	t.Run("handles precertificate entry", func(t *testing.T) {
		leafInputFor140052 := "AAAAAAFelOdpjAAB43aJADBzoMZJzGVt6UbAMXTSXFZv48OAW4RvUjaUN5gAAtswggLXoAMCAQICBwVZdafzxKkwDQYJKoZIhvcNAQELBQAwfzELMAkGA1UEBhMCR0IxDzANBgNVBAgMBkxvbmRvbjEXMBUGA1UECgwOR29vZ2xlIFVLIEx0ZC4xITAfBgNVBAsMGENlcnRpZmljYXRlIFRyYW5zcGFyZW5jeTEjMCEGA1UEAwwaTWVyZ2UgRGVsYXkgSW50ZXJtZWRpYXRlIDEwHhcNMTcwOTE4MTIxMjI3WhcNMTcxMDAxMDI1NTUyWjBjMQswCQYDVQQGEwJHQjEPMA0GA1UEBwwGTG9uZG9uMSgwJgYDVQQKDB9Hb29nbGUgQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5MRkwFwYDVQQFExAxNTA1NzM2NzQ3MzY5NjQxMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAizCcbsTPTTGWZuFPxtnZpPTdpwnqtyS5F+0IqsVsLP+gCu6NqGJWz0WzheFNgnH/75p+NAOSvvoTpbGVbDpqNmE8PwNbwPYK7edVt2Q10U9GfstSEvj2A3WSqByuX74A7vFxkiailj76XCDc4UXxMfHi4PS6aGp76JvVEFq17GW9e7ad8zlLYOnw4ctWaxV1jmgx9s/tduPWvHfY72xXU6sKZ//n3gQ0GbmQr6EJN6dr40z8kFVIcAwd0Q9FE5Ii4qRAnlHEXGbo1KSyqRekSbbsUymFwjQMjjnU02UV+Kl3tqOwKnrqrXgbOBvri5N28zVHwvamx4HdyC7fSKZLjQIDAQABo4GLMIGIMBMGA1UdJQQMMAoGCCsGAQUFBwMBMCMGA1UdEQQcMBqCGGZsb3dlcnMtdG8tdGhlLXdvcmxkLmNvbTAMBgNVHRMBAf8EAjAAMB8GA1UdIwQYMBaAFOk8BOGAL8KEEy0mcJ7y/RrPqv7GMB0GA1UdDgQWBBTKLiXBMg6H3ol9CG0iN/BYqRtkIQAA"

		certBytes, err := extractCertificateFromLeaf(leafInputFor140052)
		if err != nil {
			// With the corrected code, this test should NOT fail.
			t.Errorf("extractCertificateFromLeaf() returned an unexpected error: %v", err)
		}

		if len(certBytes) == 0 {
			t.Error("extractCertificateFromLeaf() returned no bytes, but should have.")
		}

		base64Cert := "MIID+jCCAuKgAwIBAgIHBVl1p/PEqTANBgkqhkiG9w0BAQsFADB1MQswCQYDVQQGEwJHQjEPMA0GA1UEBwwGTG9uZG9uMTowOAYDVQQKDDFHb29nbGUgQ2VydGlmaWNhdGUgVHJhbnNwYXJlbmN5IChQcmVjZXJ0IFNpZ25pbmcpMRkwFwYDVQQFExAxNTA1NzM2NzQ3MDk3NDQ1MB4XDTE3MDkxODEyMTIyN1oXDTE3MTAwMTAyNTU1MlowYzELMAkGA1UEBhMCR0IxDzANBgNVBAcMBkxvbmRvbjEoMCYGA1UECgwfR29vZ2xlIENlcnRpZmljYXRlIFRyYW5zcGFyZW5jeTEZMBcGA1UEBRMQMTUwNTczNjc0NzM2OTY0MTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAIswnG7Ez00xlmbhT8bZ2aT03acJ6rckuRftCKrFbCz/oArujahiVs9Fs4XhTYJx/++afjQDkr76E6WxlWw6ajZhPD8DW8D2Cu3nVbdkNdFPRn7LUhL49gN1kqgcrl++AO7xcZImopY++lwg3OFF8THx4uD0umhqe+ib1RBatexlvXu2nfM5S2Dp8OHLVmsVdY5oMfbP7Xbj1rx32O9sV1OrCmf/594ENBm5kK+hCTena+NM/JBVSHAMHdEPRROSIuKkQJ5RxFxm6NSksqkXpEm27FMphcI0DI451NNlFfipd7ajsCp66q14Gzgb64uTdvM1R8L2pseB3cgu30imS40CAwEAAaOBoDCBnTATBgNVHSUEDDAKBggrBgEFBQcDATAjBgNVHREEHDAaghhmbG93ZXJzLXRvLXRoZS13b3JsZC5jb20wDAYDVR0TAQH/BAIwADAfBgNVHSMEGDAWgBSCsHkJHYJNPTdqYnnG9sRHMg3U4zAdBgNVHQ4EFgQUyi4lwTIOh96JfQhtIjfwWKkbZCEwEwYKKwYBBAHWeQIEAwEB/wQCBQAwDQYJKoZIhvcNAQELBQADggEBAGd+lssGCDLvdnruskkEY10BTyD+rkbgpzhicJ4k7UOpbDNhWDV8UyQ0RaB9DBYdTSsod9wuunD8JAYPKcckXWb3h+0PfinEhsllC00aNURX6HHwJBsVAq0hp29DXQiiP8zZxmMARf3lwHgSuAcDK/npnprtLe4UUL0HjdY/NML6L93k1QzpACkdHWgJD1culMH9v+6IIUOiwhfWC+LarDyiH2Ganb/HsazLGJoaZ1z26ADkOSLov4/YgyWwD/NoBUrYBq3QcukSf5cRMbCkDWwL+zKMKlg0cTehtepHLgyftRbiKpKwKIAK2Qt5ZcXwnNZ9OkTtBU1jv1fm6uePCxY="
		if base64.StdEncoding.EncodeToString(certBytes) != base64Cert {
			t.Errorf("extractCertificateFromLeaf() returned unexpected certificate bytes.\nExpected: %s\nGot: %s", base64Cert, base64.StdEncoding.EncodeToString(certBytes))
		}
	})
}
