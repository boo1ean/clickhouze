const Promise = require('bluebird')
const ClickHouse = require('@apla/clickhouse')

function insert (ch, table, obj) {
	return new Promise((resolve, reject) => {
		const keys = []
		const values = []

		for (const key in obj) {
			keys.push(key)
			values.push(
				typeof obj[key] === 'string'
				? escape(obj[key])
				: obj[key]
			)
		}

		const query = `INSERT INTO ${table}(${keys.join(',')}) VALUES (${values.join(',')})`
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

function escape (string) {
	return "'" + string.replace(/\\/g, '\\\\').replace(/'/g, '\\\'') + "'"
}

module.exports = function buildClient (config) {
	const ch = new ClickHouse(config)

	return {
		client: ch,
		insert: insert.bind(null, ch),
		query: query.bind(null, ch),
		querySingle: querySingle.bind(null, ch),
	}
}
