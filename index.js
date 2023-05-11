'use strict'

const fp = require('fastify-plugin')
const oracledb = require('oracledb');

async function query(sql, values = [], options = {}) {
  const conn = await this.pool.getConnection();
  try {
    const result = await conn.execute(sql, values, options);
    return result;
  } finally {
    conn.close();
  }
}

async function transact(fn) {
  const conn = await this.pool.getConnection();
  try {
    await conn.execute('BEGIN');
    const result = await fn(conn);
    await conn.execute('COMMIT');
    return result;
  } catch (err) {
    await conn.execute('ROLLBACK');
    throw err;
  } finally {
    conn.close();
  }
}

async function decorateFastifyInstance(pool, fastify, options) {
  const oracle = {
    getConnection: pool.getConnection.bind(pool),
    pool,
    query: query.bind(pool),
    transact: transact.bind(pool),
    db: oracledb,
  };

  if (options.name) {
    if (!fastify.oracle) {
      fastify.decorate('oracle', oracle);
    }

    if (fastify.oracle[options.name]) {
      throw new Error(`fastify-oracle: connection name "${options.name}" has already been registered`);
    }

    fastify.oracle[options.name] = oracle;
  } else {
    if (fastify.oracle) {
      throw new Error('fastify-oracle has already been registered');
    } else {
      fastify.decorate('oracle', oracle);
    }
  }

  fastify.addHook('onClose', async (_, done) => {
    await pool.close(options.drainTime);
    done();
  });
}

async function fastifyOracleDB(fastify, options) {
  if (options.client) {
    if (!oracledb.Pool.prototype.isPrototypeOf(options.client)) {
      throw new Error('fastify-oracle: supplied client must be an instance of oracledb.pool');
    }
    return decorateFastifyInstance(options.client, fastify, options);
  }

  if (options.poolAlias) {
    try {
      const pool = await oracledb.getPool(options.poolAlias);
      return decorateFastifyInstance(pool, fastify, options);
    } catch (err) {
      throw new Error(`fastify-oracle: could not get pool alias - ${err.message}`);
    }
  }

  if (!options.pool) {
    throw new Error('fastify-oracle: must supply options.pool oracledb pool options');
  }

  if (options.outFormat) {
    oracledb.outFormat = oracledb[options.outFormat.toUpperCase()];
  }

  if (options.fetchAsString) {
    oracledb.fetchAsString = options.fetchAsString.map((t) => oracledb[t.toUpperCase()]);
  }

  const pool = await oracledb.createPool(options.pool);
  return decorateFastifyInstance(pool, fastify, options);
}



module.exports = fp(fastifyOracleDB, {
  fastify: '>=3.0.0',
  name: 'fastify-oracle',
});
