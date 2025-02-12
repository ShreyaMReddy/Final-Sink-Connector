$messages = @'
{"id": "TECH007", "name": "Smart Watch", "category": "Electronics", "price": 299.99, "tags": ["tech", "wearable", "fitness"]}
{"id": "BOOK006", "name": "Artificial Intelligence", "category": "Books", "price": 79.99, "tags": ["tech", "education", "AI"]}
{"id": "FOOD005", "name": "Premium Tea Set", "category": "Food", "price": 45.99, "tags": ["beverage", "gift", "luxury"]}
{"id": "TECH008", "name": "Mechanical Keyboard", "category": "Electronics", "price": 159.99, "tags": ["tech", "gaming", "accessories"]}
'@

$messages -split "`n" | ForEach-Object { 
    $message = $_
    if ($message.Trim()) {
        $message | Out-File -Encoding utf8 -NoNewline temp_message.txt
        docker exec -i kafka kafka-console-producer --broker-list kafka:9092 --topic products < temp_message.txt
        Remove-Item temp_message.txt
    }
}
