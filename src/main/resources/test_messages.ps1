$messages = @'
{"id": "TECH001", "name": "Laptop Pro", "category": "Electronics", "price": 1299.99, "tags": ["tech", "computer"]}
{"id": "BOOK001", "name": "Python Programming", "category": "Books", "price": 49.99, "tags": ["programming", "education"]}
{"id": "FOOD001", "name": "Organic Coffee", "category": "Food", "price": 15.99, "tags": ["organic", "beverage"]}
{"id": "TECH002", "name": "Wireless Mouse", "category": "Electronics", "price": 29.99, "tags": ["tech", "accessories"]}
{"id": "BOOK002", "name": "Data Science Guide", "category": "Books", "price": 39.99, "tags": ["data", "education"]}
{"id": "CLOTH001", "name": "Cotton T-Shirt", "category": "Clothing", "price": 19.99, "tags": ["fashion", "casual"]}
{"id": "HOME001", "name": "Vacuum Cleaner", "category": "Home Appliances", "price": 199.99, "tags": ["cleaning", "electronics"]}
{"id": "SPORTS001", "name": "Yoga Mat", "category": "Sports", "price": 29.99, "tags": ["fitness", "exercise"]}
{"id": "TOY001", "name": "Lego Set", "category": "Toys", "price": 59.99, "tags": ["kids", "building"]}
{"id": "TECH003", "name": "Smartphone X", "category": "Electronics", "price": 999.99, "tags": ["tech", "mobile"]}
{"id": "FOOD002", "name": "Dark Chocolate", "category": "Food", "price": 5.99, "tags": ["snack", "sweets"]}
{"id": "BOOK003", "name": "Machine Learning Basics", "category": "Books", "price": 59.99, "tags": ["AI", "education"]}
{"id": "CLOTH002", "name": "Leather Jacket", "category": "Clothing", "price": 149.99, "tags": ["fashion", "winter"]}
{"id": "HOME002", "name": "Coffee Maker", "category": "Home Appliances", "price": 89.99, "tags": ["kitchen", "beverage"]}
{"id": "TECH004", "name": "4K Monitor", "category": "Electronics", "price": 399.99, "tags": ["tech", "display"]}
{"id": "SPORTS002", "name": "Tennis Racket", "category": "Sports", "price": 79.99, "tags": ["sports", "tennis"]}
{"id": "FOOD003", "name": "Green Tea", "category": "Food", "price": 8.99, "tags": ["beverage", "healthy"]}
{"id": "CLOTH003", "name": "Denim Jeans", "category": "Clothing", "price": 59.99, "tags": ["fashion", "casual"]}
{"id": "BOOK004", "name": "Cloud Computing Guide", "category": "Books", "price": 45.99, "tags": ["tech", "education"]}
{"id": "TOY002", "name": "Remote Control Car", "category": "Toys", "price": 39.99, "tags": ["kids", "electronic"]}
{"id": "HOME003", "name": "Air Purifier", "category": "Home Appliances", "price": 159.99, "tags": ["health", "appliance"]}
{"id": "TECH005", "name": "Wireless Earbuds", "category": "Electronics", "price": 129.99, "tags": ["tech", "audio"]}
{"id": "SPORTS003", "name": "Dumbbells Set", "category": "Sports", "price": 89.99, "tags": ["fitness", "weights"]}
{"id": "FOOD004", "name": "Protein Bars", "category": "Food", "price": 24.99, "tags": ["healthy", "snack"]}
{"id": "CLOTH004", "name": "Winter Boots", "category": "Clothing", "price": 89.99, "tags": ["fashion", "winter"]}
{"id": "TECH006", "name": "Gaming Console", "category": "Electronics", "price": 499.99, "tags": ["tech", "gaming"]}
{"id": "HOME004", "name": "Robot Vacuum", "category": "Home Appliances", "price": 299.99, "tags": ["smart", "cleaning"]}
{"id": "BOOK005", "name": "Blockchain Basics", "category": "Books", "price": 34.99, "tags": ["tech", "cryptocurrency"]}
{"id": "TOY003", "name": "Art Set", "category": "Toys", "price": 29.99, "tags": ["creative", "kids"]}
{"id": "SPORTS004", "name": "Basketball", "category": "Sports", "price": 24.99, "tags": ["sports", "ball"]}
'@

$messages -split "`n" | ForEach-Object {
    $_ | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:9092 --topic products
}
