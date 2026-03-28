const orders = {
  "ORD-1001": {
    id: "ORD-1001",
    status: "Delivered",
    items: ["Wireless Headphones", "USB-C Cable"],
    total: 89.99,
    deliveredAt: "2026-03-25",
    trackingNumber: "TRK-9834721"
  },
  "ORD-1002": {
    id: "ORD-1002",
    status: "Shipped",
    items: ["Laptop Stand"],
    total: 45.00,
    estimatedDelivery: "2026-03-30",
    trackingNumber: "TRK-5523891"
  },
  "ORD-1003": {
    id: "ORD-1003",
    status: "Processing",
    items: ["Mechanical Keyboard"],
    total: 129.99,
    estimatedDelivery: "2026-04-02",
    trackingNumber: null
  },
  "ORD-1004": {
    id: "ORD-1004",
    status: "Cancelled",
    items: ["Mouse Pad"],
    total: 19.99,
    cancelledAt: "2026-03-26"
  }
};

const accounts = {
  "user@example.com": {
    name: "Alex Johnson",
    email: "user@example.com",
    memberSince: "2024-01-15",
    loyaltyPoints: 340,
    orders: ["ORD-1001", "ORD-1002"]
  },
  "jane@example.com": {
    name: "Jane Smith",
    email: "jane@example.com",
    memberSince: "2025-06-10",
    loyaltyPoints: 85,
    orders: ["ORD-1003", "ORD-1004"]
  }
};

module.exports = { orders, accounts };
