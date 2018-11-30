import redis

if __name__ == '__main__':
    pool=redis.ConnectionPool(host='120.27.241.54',port=6379,decode_responses=True)

    r=redis.Redis(connection_pool=pool)

    # r.set('apple','a')
    print(r.get('68'))