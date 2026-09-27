/// Prefix sizes to try, largest first, while preserving ledger order.
pub fn prefix_lengths(candidate_count: usize) -> impl Iterator<Item = usize> {
    (1..=candidate_count).rev()
}
