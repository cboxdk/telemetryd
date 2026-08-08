# Test fixtures

## `oidc-test-key.der`

**A throwaway RSA-2048 private key, generated for this test suite and used nowhere
else.** It signs tokens in `crates/server/src/oidc.rs`'s tests so that verification
runs against a real signature rather than a mock that returns success — which is the
only way key parsing, algorithm selection, claim validation and scope mapping are
proved to agree.

It is committed rather than generated per run so the suite does not spend a second on
key generation and does not vary between runs.

If a secret scanner flags this file: it is a true positive for "a private key is in the
repository" and a false positive for "a secret leaked". Nothing has ever been protected
by it. It cannot be rotated because it guards nothing.
