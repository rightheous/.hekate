use hekate::runtime::sleep_batch::prefix_lengths;

#[test]
fn sleep_batch_attempts_prefixes_from_largest_to_smallest() {
    assert_eq!(prefix_lengths(3).collect::<Vec<_>>(), vec![3, 2, 1]);
    assert!(prefix_lengths(0).next().is_none());
}
