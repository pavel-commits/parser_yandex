# -*- coding: utf-8 -*-
import pymysql
from config import host, user, password, database, timeout, executable_path


def date_update():
    try:
        if host and user and password and database:
            connection = pymysql.connect(host=host,
                                         user=user,
                                         port=3306,
                                         password=password,
                                         database=database)
            print("DATABASE CONNECTION SUCCESS")
            cursor = connection.cursor()
            cursor.execute("""UPDATE `sFeedback_site_url`
                    SET `dateLastProceed` = ADDDATE(NOW(), INTERVAL - 10 HOUR)""")
            connection.commit()

            return "SUCCESS"
        else:
            return "FAIL"
    except Exception as e:
        print("ERROR!")
        print("DATABASE CONNECTION FAILED")
        print(e)
        return "FAIL"


def main():
    answer = date_update()
    print(answer)


if __name__ == '__main__':
    main()

