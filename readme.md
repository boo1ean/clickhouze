## clickhouze

Naive clickhouse client

### Installation

```
npm i clickhouze
```

### Usage

```javascript
const client = require('clickhouze')({
	host: '127.0.0.1',
	port: 8123,
})

client.insert('events', { category: 'a', tag1: '2', tag2: '3' })
	.then(() => console.log('inserted'))

client.query('select count() from events')
	.then(result => console.log(result))

client.batchInsert('events', [{ a: 1 }, { b: 2 }, { c: 3 }])
```
