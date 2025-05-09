import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
#import pandas as pd

"""
본 컨트롤로 호출하는데서 import pandas as pd 해서 처리하면 좋음
pd_dt = pd.DataFrame(result['datas'])
        print(pd_dt)

        pd_dt.to_csv(r'mysql_output_pandas.csv', index=False)

        # 인덱스 보기
        print(pd_dt.columns)

        # 0~2열 보기
        print(pd_dt.iloc[:, 0:3])

        # 1, 4, 6행 보기
        print(pd_dt.iloc[[1, 4, 6]])
        print(pd_dt.iloc[[1, 4, 6], :])
        
여기서부터 본 컨트롤러 사용법

mysqlctrl = MysqlController("112.216.49.132", "watosys", "wato1234", "levelhanja")

        conditions = {'tablename': 'browworddetail as a'}


        conditions['select'] = "a.*"
        conditions['orderby'] = " order by doceum "
        conditions['offset'] = "1"
        conditions['limit'] = "10"

        conditions['and_where'] = {}
        conditions['and_where']['a.strkey'] = '271805'
        conditions['and_where']['a.level'] = '11'

        conditions['or_where'] = {}
        conditions['or_where']['a.strkey'] = '271805'
        conditions['or_where']['a.level'] = '11'

        conditions['search_like'] = {}
        conditions['search_like']['a.strkey'] = '271805'
        conditions['search_like']['a.level'] = '11'

        conditions['join'] = {}
        conditions['join'][0] = {'tbl': 'browwordlist AS b', 'cont' : 'b.hanjakey=a.strkey', 'where' : 'left' }
        conditions['join'][1] = {'tbl': 'browwordlist AS c', 'cont': 'c.hanjakey=a.strkey', 'where': 'left'}

        conditions['and_where_string'] = "strkey = '2711805' AND a.level = '11'"

        conditions['group_by'] = {}
        conditions['group_by'][0] = 'a.level'
        conditions['group_by'][1] = 'strkey'

        conditions['having'] = 'strkey > 2'
        
        conditions['between'] = {'col': 'hanja', 'to': '0', 'from': 'z'}

        result = mysqlctrl.base_select(conditions)
        pd_dt = pd.DataFrame(result['datas'])
        print(pd_dt)

        pd_dt.to_csv(r'mysql_output_pandas.csv', index=False)

        # 인덱스 보기
        print(pd_dt.columns)

        # 0~2열 보기
        print(pd_dt.iloc[:, 0:3])

        # 1, 4, 6행 보기
        print(pd_dt.iloc[[1, 4, 6]])
        print(pd_dt.iloc[[1, 4, 6], :])

        conditions = {'tablename': 'browworddetail'}

        datas = {}
        datas['strkey'] = '1000002'
        datas['hanja'] = 'aaa'
        datas['vocabulary'] = 'bbb'
        datas['levelname'] = 'ccc'
        datas['abilitylevel'] = 'aaa'

        mysqlctrl.base_insert(conditions, datas)

        conditions['and_where'] = {}
        conditions['and_where']['strkey'] = '1000002'

        datas = {}

        datas['hanja'] = '111'
        datas['vocabulary'] = '222'
        datas['levelname'] = '333'
        datas['abilitylevel'] = '444'

        mysqlctrl.base_update(conditions, datas)

        conditions['and_where'] = {}
        conditions['and_where']['strkey'] = '1000002'

        mysqlctrl.base_delete(conditions)

        mysqlctrl.close()
        
결과는 pandas를 사용하여 Column 명을 할당하고, 데이터를 쉽게 분석하고 싶기도 합니다.
"""

class PostgreController:
    
    def __init__(self, host, port, id, pw, db_name):
        self.conn = psycopg2.connect(host=host, user=id, password=pw, port=port, dbname=db_name)
        self.conn.autocommit = True
        self.curs = self.conn.cursor(cursor_factory=RealDictCursor)

    def close(self):
        self.conn.close()

    def insert_total(self, total):
        sql = 'INSERT INTO entire_nodes (count_of_nodes) VALUES (%s)'
        self.curs.execute(sql, (total,))
        self.conn.commit()

    def base_select(self, condition):
        stmtSelect = ""
        stmtWhere = ""
        stmtOrder = ""

        #if condition.get('select') is not None:
        #    stmtSelect = "SELECT " + condition['select'] + " FROM " + condition['tablename']
        #else:
        #    stmtSelect = "SELECT * FROM " + condition['tablename']

            #stmtSelectCount = "SELECT count(*) FROM " + condition['tablename']

        if condition.get('select_string') is not None:
            stmtSelect = condition['select_string']
            stmtSelectCount = condition['select_count']
        else:
            stmtSelect = "SELECT " + condition['select'] + " FROM " + condition['tablename']
            stmtSelectCount = "SELECT count(*) FROM " + condition['tablename']



        """
        order by
        """
        if condition.get('orderby'):
            stmtOrder = condition['orderby']


        """
        and where string
        """
        if condition.get('and_where_string'):
            if 'WHERE' in stmtWhere or 'where' in stmtWhere:
                stmtWhere = " AND " + condition['and_where_string']
            else:
                stmtWhere = " WHERE " + condition['and_where_string']

        """
        and where
        """
        isParentheses = False
        if condition.get('and_where'):
            if 'WHERE' in stmtWhere or 'where' in stmtWhere:
                stmtWhere += " AND ( "
                isParentheses = True
            else:
                stmtWhere = " WHERE ( "
                isParentheses = False

            for idx, key in enumerate(condition['and_where']):
                if idx == 0:
                    stmtWhere += key + " = \"" + condition['and_where'][key] + "\" "
                else:
                    stmtWhere += " AND " + key + " = \"" + condition['and_where'][key] + "\" "
            
            stmtWhere += " ) "

        """
        or where string
        """
        if condition.get('or_where_string'):
            if 'WHERE' in stmtWhere or 'where' in stmtWhere:
                stmtWhere = " OR " + condition['or_where_string'] + " "
            else:
                stmtWhere = " WHERE " + condition['or_where_string'] + " "

        """
        or_where
        """
        if condition.get('or_where'):
            if 'WHERE' in stmtWhere or 'where' in stmtWhere:
                stmtWhere += " OR ( "
            else:
                stmtWhere += " WHERE ( "

            for idx, key in enumerate(condition['or_where']):
                if idx == 0:
                    stmtWhere += key + " = \"" + condition['or_where'][key] + "\" "
                else:
                    stmtWhere += " OR " + key + " = \"" + condition['or_where'][key] + "\" "
            stmtWhere += " ) "

        """
        BETWEEN
        """
        if condition.get('between'):
            if 'WHERE' in stmtWhere or 'where' in stmtWhere:
                stmtWhere += " AND ( "
            else:
                stmtWhere += " WHERE ( "

            stmtWhere += condition['between']['col'] + " BETWEEN " + condition['between']['from'] + " AND " + condition['between']['to']
            stmtWhere += ") "

        """
        like
        """
        if condition.get('search_like'):
            sl_count = condition['search_like']
            if 'WHERE' in stmtWhere or 'where' in stmtWhere:
                stmtWhere += ' AND ( '
            else:
                stmtWhere += " WHERE ( "

            for idx, key in enumerate(condition['search_like']):
                if idx == 0:
                    stmtWhere += key + " LIKE \"%" + condition['search_like'][key] + "%\" "
                else:
                    stmtWhere += " OR " + key + " LIKE \"%" + condition['search_like'][key] + "%\" "
            stmtWhere += ") "

        """
        join
        """
        stmtJoin = " "
        if condition.get('join'):
            sl_count = condition['join']
            for idx, key in enumerate(condition['join']):
                value = condition['join'][key]
                # {'tbl': 'member AS c', 'cont': 'c.user_id=a.user_id', 'where': 'left'}
                stmtJoin += value['where'] + " JOIN " + value['tbl'] + " ON " + value['cont'] + " "

        """
        group_by
        """
        stmtGroup = " "
        if condition.get('group_by'):
            sl_count = len(condition['group_by'])
            for idx, key in enumerate(condition['group_by']):
                value = condition['join'][key]
                if idx == sl_count-1:
                    stmtGroup += condition['group_by'][key]
                else:
                    stmtGroup += condition['group_by'][key] + ", "
        """
        having
        """
        if condition.get('having'):
            stmtGroup += " HAVING " + condition['having']

        sql = stmtSelect + stmtJoin + stmtWhere + stmtGroup

        """
        LIMIT
        """
        stmtLimit = ""
        if condition.get('limit') and condition.get('offset'):
            stmtLimit += " LIMIT " + condition['offset'] + " ," + condition['limit']
        else:
            if condition.get('limit'):
                stmtLimit += " LIMIT " + condition['limit']


        result = {'totalcount':0, 'datas':{}}

        # total count
        sql = stmtSelectCount + stmtJoin + stmtWhere
        try:
            try:
                self.curs.execute(sql)
                rst = self.curs.fetchall()
                result['totalcount'] = rst[0]['count(*)']
            except Exception as e:
                if e.args[0] == "":
                    pass
                else:
                    raise
        except Exception as e:
            pass

        sql = stmtSelect + stmtJoin + stmtWhere + stmtGroup + stmtLimit

        print(sql)

        try:
            try:
                self.curs.execute(sql)
                rst = self.curs.fetchall()
                result['datas'] = rst
            except Exception as e:
                if e.args[0] == "":
                    pass
                else:
                    raise
        except Exception as e:
            pass

        return result

    def base_insert(self, condition, datas):
        """
        주어진 테이블에 데이터를 삽입하는 함수입니다.

        :param condition: 데이터를 삽입할 조건,  테이블 이름지정에 사용
        :param data: 삽입할 데이터 (딕셔너리 형태)
        """
        """
        with self.conn.cursor() as cursor:
            columns = datas.keys()
            values = [datas[column] for column in columns]
            
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(condition['tablename']),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(values))
            )
            
            cursor.executemany(insert_query, values)
            self.conn.commit()

        """
        sql = "INSERT INTO " + condition['tablename'] + "("

        sl_count = len(datas)
        for idx, key in enumerate(datas):
            if idx == sl_count - 1:
                sql += key + " "
            else:
                sql += key + ", "
        sql += ")"

        sql += " VALUES("
        for idx, key in enumerate(datas):
            if idx == sl_count - 1:
                sql += "\'" + datas[key] + "\'"
            else:
                sql += "\'" + datas[key] + "\', "

        sql += ")"

        #print(sql)

        try:
            try:
                self.curs.execute(sql)
                self.conn.commit()
            except Exception as e:
                #print("Exception INSERT self.curs.execute  {0} \n {1}".format(e, sql))
                if e.args[0] == "":
                    pass
                else:
                    raise
        except Exception as e:
            #print("Exception INSERT {0} \n {1}".format(e, sql))
            pass
        
    def base_update(self, condition, datas):
        sql = ""
        stmtWhere = ""

        sql = "UPDATE " + condition['tablename'] + " SET "

        sl_count = len(datas)
        for idx, key in enumerate(datas):
            if idx == sl_count - 1:
                sql += key + " = \"" + datas[key] + "\" "
            else:
                sql += key + " = \"" + datas[key] + "\", "

        """
        and where
        """
        if condition.get('and_where'):
            if 'WHERE' in stmtWhere:
                pass
            else:
                stmtWhere = " WHERE "

            for idx, key in enumerate(condition['and_where']):
                if idx == 0:
                    stmtWhere += key + " = \"" + condition['and_where'][key] + "\" "
                else:
                    stmtWhere += " AND " + key + " = \"" + condition['and_where'][key] + "\" "

        """
        or_where
        """
        if condition.get('or_where'):
            if 'WHERE' in stmtWhere:
                stmtWhere += " AND ("
            else:
                stmtWhere += " WHERE ( "

            for idx, key in enumerate(condition['or_where']):
                if idx == 0:
                    stmtWhere += key + " = \"" + condition['or_where'][key] + "\" "
                else:
                    stmtWhere += " OR " + key + " = \"" + condition['or_where'][key] + "\" "
            stmtWhere += ") "

        try:
            try:
                self.curs.execute(sql + stmtWhere)
                self.conn.commit()
            except Exception as e:
                print("Exception UPDATE self.curs.execute {0} \n {1}".format(e, sql))
                if e.args[0] == "":
                    pass
                else:
                    raise
        except Exception as e:
            print("Exception UPDATE {0} \n {1}".format(e, sql))
            pass
        

    def base_delete(self, condition):
        sql = ""
        stmtWhere = ""

        sql = "DELETE FROM " + condition['tablename'] + " "

        """
        and where
        """
        if condition.get('and_where'):
            if 'WHERE' in stmtWhere:
                pass
            else:
                stmtWhere = " WHERE "

            for idx, key in enumerate(condition['and_where']):
                if idx == 0:
                    stmtWhere += key + " = \"" + condition['and_where'][key] + "\" "
                else:
                    stmtWhere += " AND " + key + " = \"" + condition['and_where'][key] + "\" "

        """
        or_where
        """
        if condition.get('or_where'):
            if 'WHERE' in stmtWhere:
                stmtWhere += " AND ("
            else:
                stmtWhere += " WHERE ( "

            for idx, key in enumerate(condition['or_where']):
                if idx == 0:
                    stmtWhere += key + " = \"" + condition['or_where'][key] + "\" "
                else:
                    stmtWhere += " OR " + key + " = \"" + condition['or_where'][key] + "\" "
            stmtWhere += ") "

        try:
            try:
                self.curs.execute(sql + stmtWhere)
                self.conn.commit()
            except Exception as e:
                print("Exception DELETE self.curs.execute {0} \n {1}".format(e, sql))
                if e.args[0] == "":
                    pass
                else:
                    raise
        except Exception as e:
            print("Exception DELETE {0} \n {1}".format(e, sql))
            pass      