local engine = ...

if engine ~= 'vinyl' and engine ~= 'memtx' then
    error('Unsupported KV engine: ' .. tostring(engine))
end

local space = box.schema.space.create('KV', {
    engine = engine,
    if_not_exists = true,
})

if space.engine ~= engine then
    error('Existing KV space engine mismatch: expected ' .. engine .. ', got ' .. tostring(space.engine))
end

space:format({
    { name = 'key', type = 'string' },
    { name = 'value', type = 'varbinary', is_nullable = true },
})

space:create_index('primary', {
    type = 'TREE',
    unique = true,
    if_not_exists = true,
    parts = {
        { field = 1, type = 'string' },
    },
})

return {
    space.name,
    space.engine,
    space.index.primary.type,
    space.index.primary.unique,
}
