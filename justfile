inspect-private-release:
    TEST_ZCASHD_COMPAT=1 cargo test -p zakura --test acceptance zcashd_compat_inspect_private_release --features privacy-admission -- --ignored --exact --nocapture --test-threads=1
