"""Behavioral checks for sequencing, retries, and storage-enforced fencing."""

from concurrent.futures import ThreadPoolExecutor
from itertools import permutations
import unittest

from order_simulation import Event, OrderStore


class OrderStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = OrderStore()
        self.store.create("order")
        self.token = self.store.acquire_token("order")
        self.paid = Event("order", 1, "PAID")
        self.shipped = Event("order", 2, "SHIPPED")

    def test_required_delivery_order(self):
        self.store.deliver(self.shipped, self.token)
        self.assertEqual(self.store.snapshot("order"), (0, "CREATED", (2,)))
        messages = self.store.deliver(self.paid, self.token)
        self.assertEqual(messages, ["APPLY order=order v1 PAID", "APPLY order=order v2 SHIPPED"])
        self.assertIn("IGNORE duplicate", self.store.deliver(self.shipped, self.token)[0])
        self.assertEqual(self.store.snapshot("order"), (2, "SHIPPED", ()))

    def test_missing_version_remains_pending(self):
        messages = self.store.deliver(self.shipped, self.token)
        self.assertIn("missing v1", messages[0])
        self.assertEqual(self.store.snapshot("order"), (0, "CREATED", (2,)))

    def test_duplicate_pending_event(self):
        self.store.deliver(self.shipped, self.token)
        self.assertIn("already pending", self.store.deliver(self.shipped, self.token)[0])
        self.assertEqual(self.store.snapshot("order"), (0, "CREATED", (2,)))

    def test_new_owner_fences_old_worker_before_any_new_write(self):
        newer = self.store.acquire_token("order")
        self.assertGreater(newer, self.token)
        self.assertIn("REJECT", self.store.deliver(self.paid, self.token)[0])
        self.assertEqual(self.store.snapshot("order"), (0, "CREATED", ()))
        self.store.deliver(self.paid, newer)
        self.assertIn("REJECT", self.store.deliver(self.shipped, self.token)[0])
        self.assertEqual(self.store.snapshot("order"), (1, "PAID", ()))
        self.store.deliver(self.shipped, newer)
        self.assertEqual(self.store.snapshot("order"), (2, "SHIPPED", ()))

    def test_unissued_token_cannot_write(self):
        self.assertIn("REJECT", self.store.deliver(self.paid, self.token + 1)[0])
        self.assertEqual(self.store.snapshot("order"), (0, "CREATED", ()))

    def test_invalid_lifecycle_does_not_change_state(self):
        for event in [Event("order", 1, "SHIPPED"), Event("order", 3, "PAID")]:
            with self.assertRaises(ValueError):
                self.store.deliver(event, self.token)
        self.assertEqual(self.store.snapshot("order"), (0, "CREATED", ()))

    def test_all_delivery_permutations_with_duplicates(self):
        for deliveries in set(permutations([self.paid, self.shipped, self.shipped])):
            with self.subTest(deliveries=deliveries):
                store = OrderStore()
                store.create("order")
                token = store.acquire_token("order")
                messages = [m for event in deliveries for m in store.deliver(event, token)]
                self.assertEqual(sum(m.startswith("APPLY") for m in messages), 2)
                self.assertEqual(store.snapshot("order"), (2, "SHIPPED", ()))

    def test_concurrent_duplicate_delivery_applies_each_version_once(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(
                lambda event: self.store.deliver(event, self.token),
                [self.shipped, self.paid] * 20,
            ))
        self.assertEqual(sum(m.startswith("APPLY") for result in results for m in result), 2)
        self.assertEqual(self.store.snapshot("order"), (2, "SHIPPED", ()))

    def test_orders_have_independent_gaps(self):
        self.store.create("other")
        other_token = self.store.acquire_token("other")
        self.store.deliver(self.shipped, self.token)
        self.store.deliver(Event("other", 1, "PAID"), other_token)
        self.assertEqual(self.store.snapshot("order"), (0, "CREATED", (2,)))
        self.assertEqual(self.store.snapshot("other"), (1, "PAID", ()))


if __name__ == "__main__":
    unittest.main()
