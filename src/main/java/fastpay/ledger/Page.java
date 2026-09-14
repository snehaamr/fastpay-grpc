package fastpay.ledger;

import java.util.List;

public record Page<T>(List<T> items, String nextPageToken) {
    public boolean hasNextPage() {
        return nextPageToken != null && !nextPageToken.isBlank();
    }
}
