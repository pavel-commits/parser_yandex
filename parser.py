# -*- coding: utf-8 -*-

import random
from os import path, makedirs
import pymysql
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from config import host, user, password, database, timeout, executable_path
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException


def get_connection():
    try:
        if host and user and password and database:
            connection = pymysql.connect(host=host,
                                         user=user,
                                         port=3306,
                                         password=password,
                                         database=database)
            print("DATABASE CONNECTION SUCCESS")
            return connection
        else:
            return "FAIL"
    except Exception as e:
        print("ERROR! function get_connection()")
        print("DATABASE CONNECTION FAILED")
        print(e)
        return "FAIL"


def select_links(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT
  u.`id` AS feedbackUrlID,
  u.`platformID`,
  u.`linkName`,
  u.`linkUrl`
FROM
  `sFeedback_site_url` AS u
WHERE u.`dateLastProceed` <= ADDDATE(NOW(), INTERVAL - 6 HOUR)
""")
        links = cursor.fetchall()
        if not links:
            return "FAIL"
        cursor.execute("""UPDATE `sFeedback_site_url`
        SET `dateLastProceed` = NOW()""")
        connection.commit()

        return links
    except Exception as e:
        print("ERROR! function select_links(connection)")
        print(e)
        return "FAIL"


def select_and_make_proxies(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT
  proxyType,
  proxy,
  proxyPort,
  proxyLogin,
  proxyPass  
FROM
  `sFeedback_site_proxy`
WHERE isActive = 1
  AND dateNotWorkTime <= ADDDATE(NOW(), INTERVAL - 3 MINUTE)
  AND dateBlockTime<= ADDDATE(NOW(), INTERVAL - 15 MINUTE)
  ORDER BY dateLastUse

""")
        proxies = cursor.fetchall()

        cursor.execute("""UPDATE `sFeedback_site_proxy`
        SET `dateLastUse` = NOW()
""")
        connection.commit()

        answer_proxies = []
        for proxy in proxies:
            proxyType, proxy, proxyPort, proxyLogin, proxyPass = proxy
            ready_proxy = f"{proxyType}://{proxyLogin}:{proxyPass}@{proxy}:{proxyPort}"
            answer_proxies.append(ready_proxy)
        return answer_proxies
    except Exception as e:
        print("ERROR! function select_and_make_proxies(connection)")
        print(e)
        return "FAIL"


def update_date_last_proceed(connection):
    try:
        cursor = connection.cursor()

        cursor.execute("""UPDATE `sFeedback_site_url`
        SET `dateLastProceed` = NOW()""")
        connection.commit()

        return "SUCCESS"
    except Exception as e:
        print("ERROR! function update_date_last_proceed(connection)")
        print(e)
        return "FAIL"


def update_date_last_success(connection):
    try:
        cursor = connection.cursor()

        cursor.execute("""UPDATE `sFeedback_site_url`
        SET `dateLastSuccess` = NOW()""")
        connection.commit()

        return "SUCCESS"
    except Exception as e:
        print("ERROR! function update_date_last_success(connection)")
        print(e)
        return "FAIL"


def insert_stat(connection, row):
    try:
        feedbackUrlID = row["feedbackUrlID"]
        rateNum = row["rateNum"]
        feedbackRateNum = row["feedbackRateNum"]
        feedbackNum = int(row["feedbackNum"])
    except KeyError:
        feedbackUrlID = row["feedbackUrlID"]
        rateNum = row["rateNum"]
        feedbackRateNum = "null"
        feedbackNum = int(row["feedbackNum"])

    try:
        cursor = connection.cursor()
        cursor.execute(f"""INSERT INTO `sFeedback_site_stat`
    (`feedbackUrlID`,
    `statDate`,
    `rateNum`,
    `feedbackRateNum`,
    `feedbackNum`)
    VALUES
    ({feedbackUrlID},
    NOW(),
    {rateNum},
    {feedbackRateNum},
    {feedbackNum});""")
        connection.commit()
    except Exception as e:
        print("ERROR! function insert_stat(connection, row)")
        print(e)


def insert_feedbacks_list(connection, row):
    try:
        feedbackUrlID = row["feedbackUrlID"]
        feedbackKey = row["feedbackKey"]
        userName = row["userName"]
        userProfileUrl = row["userProfileUrl"]
        rate = row["rate"]
        feedbackTxt = row["feedbackTxt"]
        if "dateFeedback" in row.keys():
            dateFeedback = row["dateFeedback"].split(".")[0].replace("T", " ")
            cursor = connection.cursor()
            cursor.execute(f"""INSERT INTO `sFeedback_site`
                        (`feedbackUrlID`,
                        `feedbackKey`,
                        `userName`,
                        `userProfileUrl`,
                        `rate`,
                        `feedbackTxt`,
                        `dateFeedback`,
                        `dateCreated`,
                        `dateActive`)
                        VALUES
                        ({feedbackUrlID},
                        MD5('{feedbackKey}'),
                        '{userName}',
                        '{userProfileUrl}',
                        {rate},
                        '{feedbackTxt}',
                        '{dateFeedback}',
                        NOW(),
                        NOW());""")
            connection.commit()
        else:
            cursor = connection.cursor()
            cursor.execute(f"""INSERT INTO `sFeedback_site`
    (`feedbackUrlID`,
    `feedbackKey`,
    `userName`,
    `userProfileUrl`,
    `rate`,
    `feedbackTxt`,
    `dateFeedback`,
    `dateCreated`,
    `dateActive`)
    VALUES
    ({feedbackUrlID},
    MD5('{feedbackKey}'),
    '{userName}',
    '{userProfileUrl}',
    {rate},
    '{feedbackTxt}',
    NOW(),
    NOW(),
    NOW());""")
        connection.commit()
    except pymysql.err.IntegrityError:
        cursor = connection.cursor()
        cursor.execute(f"""UPDATE sFeedback_site
                SET dateActive=NOW()
        """)
        connection.commit()
    except Exception as e:
        print("function insert_feedbacks_list()")
        print()
        print(row)
        print()
        print(e)


def parse_yandex_static(driver, feedbackUrlID, linkName):
    print(f"СТРАНИЦА {feedbackUrlID} ПО ССЫЛКЕ {linkName} ОБРАБАТЫВАЕТСЯ...")
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    try:
        feedbackNum = soup.find("div", class_="_name_reviews").find("div", class_="tabs-select-view__counter").text
    except Exception as e:
        print("function", parse_yandex_static)
        feedbackNum = None

    try:
        rateNum = (soup.find("div", class_="business-header-rating-view").find(
            "span", class_="business-rating-badge-view__rating-text _size_m").text.replace(",", "."))
    except Exception as e:
        print("function parse_yandex_static()")
        print(e)
        rateNum = None

    try:
        feedbackRateNum = int(soup.find("span", class_="business-rating-amount-view _summary").text.split()[0])
    except Exception as e:
        print("function parse_yandex_static()")
        print(e)
        feedbackRateNum = "null"

    static = {
        "feedbackUrlID": feedbackUrlID,
        "rateNum": float(rateNum),
        "feedbackRateNum": feedbackRateNum,
        "feedbackNum": int(feedbackNum)
    }

    return static


def parse_yandex_feedbacks(driver, feedbackUrlID, len_feedbacks):
    print("ВСЕГО ОТЗЫВОВ:", len_feedbacks)
    time.sleep(3)

    button = driver.find_element(by=By.CLASS_NAME, value="_name_reviews")
    if button:
        button.click()
    else:
        print("Отзывы не найдены, перезапустите")

    # NEW PAGE
    print("ОТЗЫВЫ ОБРАБАТЫВАЮТСЯ...")

    all_elements = driver.find_elements(by=By.CLASS_NAME, value="business-reviews-card-view__review")
    actions = ActionChains(driver)

    while len(all_elements) < len_feedbacks:

        actions.send_keys(Keys.PAGE_DOWN).perform()
        all_elements = driver.find_elements(by=By.CLASS_NAME, value="business-reviews-card-view__review")
        time.sleep(1)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    container = soup.find("div", class_="business-tab-wrapper _materialized")

    all_cards = container.find_all("div", class_="business-reviews-card-view__review")
    ans = []
    for card in all_cards:
        try:
            author = card.find("div", class_="business-review-view__author")

            userProfileUrl = author.find("a").get("href")
            userName = author.find("a").find("span").text
        except AttributeError:
            author = card.find("div", class_="business-review-view__author")

            userProfileUrl = None
            userName = author.find("span").text
        except Exception as e:
            print("function parse_yandex_feedbacks")
            print(e)
            userProfileUrl = None
            userName = None

        try:
            r = card.find("div", class_="business-review-view__header")

            stars_div = r.find("div", class_="business-rating-badge-view__stars")
            rate = len(list(filter(lambda x: "_empty" not in x["class"], stars_div)))

            dateFeedback = r.find("span", class_="business-review-view__date").find("meta").get("content")
        except Exception as e:
            print("function parse_yandex_feedbacks")
            print(e)
            rate = None
            dateFeedback = None

        try:
            f = card.find("div", class_="business-review-view__body")
            feedbackTxt = f.find("span", class_="business-review-view__body-text").text
        except Exception as e:
            print("function parse_yandex_feedbacks")
            print(e)
            feedbackTxt = None

        try:
            feedbackKey = userProfileUrl + dateFeedback
        except TypeError:
            feedbackKey = dateFeedback

        ans.append({
            "feedbackUrlID": feedbackUrlID,
            "feedbackKey": feedbackKey,
            "userName": userName,
            "userProfileUrl": userProfileUrl,
            "rate": rate,
            "feedbackTxt": feedbackTxt,
            "dateFeedback": dateFeedback
        })
    return ans


def parse_google_static(driver, feedbackUrlID, linkName):
    print(f"СТРАНИЦА {feedbackUrlID} ПО ССЫЛКЕ {linkName} ОБРАБАТЫВАЕТСЯ...")
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    try:
        table = soup.find("div", class_="gm2-body-2 h0ySl-wcwwM-RWgCYc")

        rateNum = float(table.find("span", class_="aMPvhf-fI6EEc-KVuj8d").text.replace(",", "."))
        feedbackNum = int(table.find("button", class_="Yr7JMd-pane-hSRGPd").text.split()[0])
    except Exception as e:
        print("function parser_google_static()")
        print(e)
        rateNum = None
        feedbackNum = None

    static = {
        "feedbackUrlID": feedbackUrlID,
        "rateNum": rateNum,
        "feedbackNum": feedbackNum,
    }
    return static


def parse_google_feedbacks(driver, feedbackUrlID, len_feedbacks):
    print("ВСЕГО ОТЗЫВОВ:", len_feedbacks)

    try:
        button = driver.find_element(by=By.CSS_SELECTOR, value="button.Yr7JMd-pane-hSRGPd")
        button.click()
    except:
        driver.refresh()
        time.sleep(4)
        button = driver.find_element(by=By.CSS_SELECTOR, value="button.Yr7JMd-pane-hSRGPd")
        button.click()

    # NEW PAGE
    print("ОТЗЫВЫ ОБРАБАТЫВАЮТСЯ...")
    time.sleep(5)
    window_before = driver.window_handles[0]

    actions = ActionChains(driver)

    all_elements = driver.find_elements(by=By.CSS_SELECTOR, value="div.ODSEW-ShBeI.NIyLF-haAclf.gm2-body-2")

    area = driver.find_element(by=By.CSS_SELECTOR, value="div.siAUzd-neVct.section-scrollbox.cYB2Ge-oHo7ed.cYB2Ge-ti6hGc")

    while len(all_elements) < len_feedbacks:
        actions.send_keys_to_element(area, Keys.END).perform()
        time.sleep(1)
        all_elements = driver.find_elements(by=By.CSS_SELECTOR, value="div.ODSEW-ShBeI.NIyLF-haAclf.gm2-body-2")
        if len(driver.window_handles) > 1:
            driver.switch_to.window(window_before)

    try:
        button_close = driver.find_element(by=By.CSS_SELECTOR, value="button.OFhamd-LgbsSe.OFhamd-LgbsSe-white-LkdAo")
        if button_close:
            button_close.click()
    except NoSuchElementException:
        pass

    if len(driver.window_handles) > 1:
        driver.switch_to.window(window_before)

    for btn in driver.find_elements(by=By.CSS_SELECTOR, value="button.ODSEW-KoToPc-ShBeI.gXqMYb-hSRGPd"):
        try:
            btn.click()
            if len(driver.window_handles) > 1:
                driver.switch_to.window(window_before)
        except:
            print("Текст не раскрыт")

    soup = BeautifulSoup(driver.page_source, "html.parser")
    all_cards = soup.find_all("div", class_="ODSEW-ShBeI NIyLF-haAclf gm2-body-2")

    ans = []
    for card in all_cards:
        try:
            feedbackKey = card.get("data-review-id")
        except Exception as e:
            feedbackKey = None

        try:
            userName = card.get("aria-label")
        except Exception as e:
            userName = None

        try:
            u = card.find("div", class_="ODSEW-ShBeI-tXxcle ODSEW-ShBeI-tXxcle-SfQLQb-menu")
            userProfileUrl = u.find("a").get("href")
        except Exception as e:
            userProfileUrl = None

        try:
            u = card.find("div", class_="ODSEW-ShBeI-jfdpUb")
            rate = int(u.find("span", class_="ODSEW-ShBeI-H1e3jb").get("aria-label").split()[0])
        except Exception as e:
            rate = None

        try:
            f = card.find("div", class_="ODSEW-ShBeI-ShBeI-content")
            feedbackTxt = f.find("span", class_="ODSEW-ShBeI-text").text.replace("\n", " ")
        except Exception as e:
            feedbackTxt = None
        ans.append({
            "feedbackUrlID": feedbackUrlID,
            "feedbackKey": feedbackKey,
            "userName": userName,
            "userProfileUrl": userProfileUrl,
            "rate": rate,
            "feedbackTxt": feedbackTxt
        })
    return ans


def main():
    connection = get_connection()
    if connection == "FAIL":
        print("_____TRY TO CHECK FILE config.py_____")
        return

    links = select_links(connection)
    if links == "FAIL":
        return

    proxies = select_and_make_proxies(connection)
    if proxies == "FAIL":
        return
    print("КОЛЛИЧЕСТВО ВЗЯТЫХ ССЫЛОК:", len(links))
    if not path.exists("prefix"):
        makedirs("prefix")

    try:
        for row in links:
            feedbackUrlID, platformID, linkName, linkUrl = row

            try:
                options = webdriver.ChromeOptions()
                options.add_argument("--headless")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-gpu")
                options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15")

                # options.add_argument('--proxy-server=%s' % proxies[randrange(len(proxies))])
                driver = webdriver.Chrome(service=Service(executable_path=executable_path),
                                          options=options)

                driver.get(linkUrl)
                time.sleep(3)
                with open("prefix/prefix_data.html", "w") as f:
                    f.write(driver.page_source)

                if platformID == 1:

                    static = parse_yandex_static(driver, feedbackUrlID, linkUrl)
                    insert_stat(connection, static)
                    r = driver.current_url.split("?")
                    reviews_link = r[0] + "reviews/?" + r[1]

                    driver.get(reviews_link)
                    all_feedbacks = parse_yandex_feedbacks(driver, feedbackUrlID, static["feedbackNum"])
                    for i in all_feedbacks:
                        insert_feedbacks_list(connection, i)
                    print("ОБРАБОТАНО ОТЗЫВОВ:", len(all_feedbacks))
                elif platformID == 2:

                    static = parse_google_static(driver, feedbackUrlID, linkUrl)
                    insert_stat(connection, static)

                    all_feedbacks = parse_google_feedbacks(driver, feedbackUrlID, static["feedbackNum"])
                    for i in all_feedbacks:
                        insert_feedbacks_list(connection, i)

                    print("ОБРАБОТАНО ОТЗЫВОВ:", len(all_feedbacks))

                driver.close()
                driver.quit()
                print("------------------")
            except Exception as e:
                print("function main() обход и запись ссылок")
                print(e)
            finally:
                my_timeout = random.randint(timeout - 5, timeout + 5)
                print("ЗАДЕРЖКА", my_timeout, "СЕКУНД")
                time.sleep(my_timeout)
                print()
        update_date_last_success(connection)

    except Exception as e:
        print("function main()")
        print(e)
    finally:
        time.sleep(timeout)


if __name__ == '__main__':
    main()

