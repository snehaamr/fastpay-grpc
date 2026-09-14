package fastpay.ledger;

import fastpay.security.Role;

public record CreatedApiKey(String label, Role role, String token) {
}
