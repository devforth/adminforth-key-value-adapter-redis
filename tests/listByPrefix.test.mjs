// The fake client enforces the ZRANGEBYLEX rules the paging relies on: bounds must be
// -/+/[/(, LIMIT takes integers, and members are ordered byte-by-byte as Redis does.
import assert from 'node:assert/strict';
import test from 'node:test';
import RedisKeyValueAdapter from '../dist/index.js';

// ZRANGEBYLEX orders byte-by-byte with memcmp(), not by UTF-16 code unit.
const byteOrder = (a, b) => Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));

function adapterWithIndex(members, values) {
  const index = new Set(members);
  const calls = [];
  const adapter = Object.create(RedisKeyValueAdapter.prototype);
  adapter.redis = {
    async zRangeByLex(_key, min, max, { LIMIT: { offset, count } }) {
      // "Valid start and stop must start with ( or [" - and - / + are min / max only.
      if (min !== '-' && min[0] !== '[' && min[0] !== '(') throw new Error(`ERR min or max not valid string range item: ${min}`);
      if (max !== '+' && max[0] !== '[' && max[0] !== '(') throw new Error(`ERR min or max not valid string range item: ${max}`);
      if (!Number.isInteger(offset) || !Number.isInteger(count)) throw new Error('ERR value is not an integer or out of range');
      calls.push({ min, count });
      const inRange = [...index].sort(byteOrder).filter((member) =>
        (min === '-' || (min[0] === '[' ? byteOrder(member, min.slice(1)) >= 0 : byteOrder(member, min.slice(1)) > 0)) &&
        (max === '+' || (max[0] === '[' ? byteOrder(member, max.slice(1)) <= 0 : byteOrder(member, max.slice(1)) < 0)));
      const from = inRange.slice(offset);
      return count < 0 ? from : from.slice(0, count);   // negative count = all remaining
    },
    async mGet(keys) { return keys.map((key) => values.get(key) ?? null); },
    async zRem(_key, staleMembers) { staleMembers.forEach((member) => index.delete(member)); },
  };
  return { adapter, index, calls };
}

test('a live-row limit skips expired index members', async () => {
  const members = Array.from({ length: 502 }, (_, index) => `user:${String(index).padStart(4, '0')}`);
  const values = new Map([[`sessions:${members[500]}`, 'first'], [`sessions:${members[501]}`, 'second']]);
  const { adapter, index, calls } = adapterWithIndex(members, values);
  assert.deepEqual(await adapter.listByPrefix('user:', 500, 'sessions'), [
    { [members[500]]: 'first' }, { [members[501]]: 'second' },
  ]);
  assert.ok(calls.length > 1);
  assert.equal(index.size, 2);
  // the cursor must be exclusive, or a live last-of-page row is re-read forever
  assert.ok(calls.slice(1).every((call) => call.min.startsWith('(')), JSON.stringify(calls));
});

test('an omitted limit reads every page', async () => {
  const members = Array.from({ length: 501 }, (_, index) => `user:${String(index).padStart(4, '0')}`);
  const values = new Map(members.map((member) => [`sessions:${member}`, member]));
  const { adapter, calls } = adapterWithIndex(members, values);
  const rows = await adapter.listByPrefix('user:', undefined, 'sessions');
  assert.equal(rows.length, 501);
  assert.equal(Object.keys(rows[500])[0], members[500]);
  assert.ok(calls.length > 1);
});

test('a page with more live rows than the remaining budget returns the first ones, in order', async () => {
  const members = Array.from({ length: 10 }, (_, i) => `user:${String(i).padStart(4, '0')}`);
  const values = new Map(members.slice(1).map((m) => [`sessions:${m}`, m]));  // user:0000 expired
  const { adapter } = adapterWithIndex(members, values);
  assert.deepEqual(await adapter.listByPrefix('user:', 3, 'sessions'), [
    { 'user:0001': 'user:0001' }, { 'user:0002': 'user:0002' }, { 'user:0003': 'user:0003' },
  ]);
});

test('stops instead of spinning when the cursor cannot advance', async () => {
  // a member Redis holds as non-UTF-8 bytes, or an index whose scores are not all equal, makes
  // ZRANGEBYLEX hand back the same page for the advanced cursor
  const members = Array.from({ length: 500 }, (_, i) => `user:${String(i).padStart(4, '0')}`);
  const values = new Map(members.map((member) => [`sessions:${member}`, member]));
  const { adapter, calls } = adapterWithIndex(members, values);
  adapter.redis.zRangeByLex = async (_key, min, _max, { LIMIT: { offset, count } }) => {
    calls.push({ min, count });
    return members.slice(offset, offset + count);
  };

  const rows = await adapter.listByPrefix('user:', 1000, 'sessions');

  assert.equal(rows.length, 500);
  assert.equal(new Set(rows.map((row) => Object.keys(row)[0])).size, 500);
  assert.equal(calls.length, 2);
});

test('a fractional limit never reaches redis as a fractional COUNT', async () => {
  const members = ['user:a', 'user:b'];
  const values = new Map([['sessions:user:a', 'A'], ['sessions:user:b', 'B']]);
  const { adapter, calls } = adapterWithIndex(members, values);

  assert.deepEqual(await adapter.listByPrefix('user:', 1.5, 'sessions'), [{ 'user:a': 'A' }]);
  assert.ok(calls.every((call) => Number.isInteger(call.count) && call.count >= 1), JSON.stringify(calls));
  assert.deepEqual(await adapter.listByPrefix('user:', 0.5, 'sessions'), []);
});
