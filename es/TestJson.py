import  json
from api2.mysql import mysql

if __name__ == '__main__':
    sql='''
    
SELECT
	t1.id AS cate1 ,
	t1. NAME AS cate1name ,
	t2.id AS cate2 ,
	t2.name as cate2name,
	t3.id AS cate3,
	t3.name as cate3name
FROM
	(
		productcate AS t1
		INNER JOIN productcate AS t2 ON t1.id = t2.parentid
	)
INNER JOIN productcate AS t3 ON t2.id = t3.parentid
WHERE
	t1.DELETEd = 0
AND t2.deleted = 0
AND t3.deleted = 0
AND floor(t3.id / 100) > 0


    '''
    dd=mysql.get(sql)
    print(dd)
    jsD={

    }