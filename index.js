const ClickHouse = require('@apla/clickhouse')

function insert (ch, table, obj) {
	return new Promise((resolve, reject) => {
		const keys = []
		const values = []

		for (const key in obj) {
			keys.push(key)
			values.push(escape(obj[key]))
		}

		const query = `insert into ${table}(${keys.join(',')}) values (${values.join(',')})`
		const stream = ch.query(query)
		stream.on('error', reject)
		stream.on('end', resolve)
	})
}

function batchInsert (ch, table, rows) {
	if (!Array.isArray(rows) || !rows.length) {
		throw new Error('Batch insert rows are missing')
	}
	return new Promise((resolve, reject) => {
		const keys = Object.keys(rows[0])
		const values = []
		rows.forEach(row => {
			values.push(Object.values(row).map(escape))
		})
		const query = `insert into ${table} (${keys.join(',')}) values ${values.map(row => `(${row.join(',')})`)}`
		const stream = ch.query(query)
		stream.on('error', reject)
		stream.on('end', resolve)
	})
}

function query (ch, sql) {
	return new Promise((resolve, reject) => {
		const stream = ch.query(sql)
		const rows = []
		stream.on('error', reject)
		stream.on('data', row => rows.push(row))
		stream.on('end', () => resolve(rows))
	})
}

function querySingle (ch, sql) {
	return query(ch, sql).then(res => {
		if (res && res.length) {
			return res[0]
		}
		return null
	})
}

function escape (val) {
	if (typeof val === 'string') {
		return "'" + val.replace(/\\/g, '\\\\').replace(/'/g, '\\\'') + "'"
	}
	return val
}

module.exports = function buildClient (config) {
	const ch = new ClickHouse(config)

	return {
		client: ch,
		insert: insert.bind(null, ch),
		batchInsert: batchInsert.bind(null, ch),
		query: query.bind(null, ch),
		querySingle: querySingle.bind(null, ch),
	}
}
