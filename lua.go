package redis_lock

// LuaCheckAndDeleteDistributionLock 判断是否拥有分布式锁的归属权，是则删除
const LuaCheckAndDeleteDistributionLock = `
  local lockerKey = KEYS[1]
  local targetToken = ARGV[1]
  local getToken = redis.call('get',lockerKey)
  if (not getToken or getToken ~= targetToken) then
    return 0
	else
		return redis.call('del',lockerKey)
  end
`

const LuaCheckAndExpireDistributionLock = `
  local lockerKey = KEYS[1]
  local targetToken = ARGV[1]
  local duration = ARGV[2]
  local getToken = redis.call('get',lockerKey)
  if (not getToken or getToken ~= targetToken) then
    return 0
	else
		return redis.call('expire',lockerKey,duration)
  end
`

// 批量扣减库存防止超卖
const LuaCheckAndUpdateUserStoreResource = `
local userStoreKey = KEYS[1]
local userCurrencyKey= KEYS[2]
local itemsToReduce = cjson.decode(ARGV[1])

local ok = true

for itemId, numToReduce in pairs(itemsToReduce) do
    local userStock=0
	if tonumber(itemId) ==1 then
	userStock = tonumber(redis.call("HGET",userCurrencyKey, itemId))
	else
    userStock = tonumber(redis.call("HGET",userStoreKey, itemId))
    end

    if userStock == nil then
        userStock = 0
    end

    if userStock < -1*numToReduce then
        ok = false
        break
    end
end

if ok then
    for itemId, numToReduce in pairs(itemsToReduce) do
		if tonumber(itemId)==1 then
        redis.call("HINCRBY", userCurrencyKey, itemId, numToReduce)
		else
		redis.call("HINCRBY", userStoreKey, itemId, numToReduce)
		end
    end
    return 1
else
    return 0
end
`

/*

 func (d *DataBases) BatchIncrUserStoreResource(uid string, resource []*config.CommonItem) (err error) {
 	userStoreKey := userStoreResource + uid
 	userCurrencyKey := userCurrencyCache + uid
 	m := make(map[int]int, len(resource))
 	for _, v := range resource {
 		m[v.ItemId] = v.ItemNumber
 	}
 	bytes, _ := json.Marshal(m)
 	args := []interface{}{string(bytes)}
 	reply := d.RDB.Eval(context.Background(), lua.LuaCheckAndUpdateUserStoreResource, []string{userStoreKey, userCurrencyKey}, args)
 	ok, err := reply.Int()
 	if ok == 1 {
 		return err
 	} else {
 		return constant.ErrorAddReward
 	}
 }



*/
