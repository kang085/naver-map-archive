"""
=============================================================================
[ 네이버 지도 장소 상세 정보 크롤러 (통합 완성본) ] 

- 통합 내용: 
  1. (n8n_v5) 요일별 영업시간 분리 정형화, 사진(이미지) URL 추출, CLI(외부 인자) 실행 지원
  2. (V23) 강력한 브라우저 메모리/크래시 방어 옵션, 상세 SNS/블로그 링크 수집, 편의시설 및 주차장 정보 추출
- 출력 형식: {검색어}.csv 파일로 각 키워드별 데이터 분리 저장
=============================================================================
"""

import os
import sys
import subprocess
import urllib.parse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from pathlib import Path
import time
import random
import logging
import re
import json
from datetime import datetime
import threading

# ===========================
# ⚙️ Configuration (설정)
# ===========================
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
PROGRESS_FILE = "progress_poi.json"
SEARCH_FILE = "검색어.csv" 
HEADLESS_MODE = True  # 진행 상황을 보려면 False, 숨기려면 True
MAX_PAGE = 5
WATCHDOG_TIMEOUT = 60
MAX_RETRY_ATTEMPTS = 3
MAX_CONSECUTIVE_FAILURES = 5

# ===========================
# 📝 Set up logging
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ===========================
# ⏱️ Watchdog Timer & Utilities
# ===========================
class WatchdogTimer:
    def __init__(self, timeout_seconds=60):
        self.timeout = timeout_seconds
        self.last_activity = time.time()
        self.is_active = False
        self.lock = threading.Lock()
        
    def reset(self):
        with self.lock: self.last_activity = time.time()
            
    def start(self):
        self.is_active = True
        self.reset()
        
    def stop(self):
        self.is_active = False
        
    def check_timeout(self):
        if not self.is_active: return False
        with self.lock:
            elapsed = time.time() - self.last_activity
            if elapsed > self.timeout:
                logging.warning(f"⚠️ Watchdog timeout! {elapsed:.1f}초 동안 활동 없음")
                return True
            return False

watchdog = WatchdogTimer(WATCHDOG_TIMEOUT)

def load_progress():
    progress_file = Path(PROGRESS_FILE)
    if progress_file.exists():
        try:
            with open(progress_file, "r", encoding="utf-8") as file: return json.load(file)
        except Exception: return {"current_search": None, "current_page": 1, "completed_searches": []}
    return {"current_search": None, "current_page": 1, "completed_searches": []}

def save_progress(search_query, current_page):
    try:
        progress = load_progress()
        progress["current_search"] = search_query
        progress["current_page"] = current_page
        progress["last_update"] = datetime.now().isoformat()
        with open(PROGRESS_FILE, "w", encoding="utf-8") as file: json.dump(progress, file, ensure_ascii=False, indent=2)
    except Exception as e: logging.error(f"진행 상태 저장 실패: {e}")

def mark_search_completed(search_query):
    try:
        progress = load_progress()
        if search_query not in progress["completed_searches"]: progress["completed_searches"].append(search_query)
        progress["current_search"] = None
        progress["current_page"] = 1
        with open(PROGRESS_FILE, "w", encoding="utf-8") as file: json.dump(progress, file, ensure_ascii=False, indent=2)
    except Exception as e: logging.error(f"완료 상태 저장 실패: {e}")

# ===========================
# 💾 CSV 데이터 저장 및 검증
# ===========================
def save_data(data, file_name):
    if not data:
        logging.warning("저장할 데이터가 없습니다.")
        return
    watchdog.reset()
    df = pd.DataFrame(data)
    
    if not os.path.isfile(file_name):
        df.to_csv(file_name, index=False, encoding="utf-8-sig")
        logging.info(f"📁 파일 {file_name} 생성 및 저장 완료 ({len(data)}건)")
    else:
        df.to_csv(file_name, mode='a', header=False, index=False, encoding="utf-8-sig")
        logging.info(f"📁 파일 {file_name}에 데이터 추가 완료 ({len(data)}건)")

# ===========================
# ☁️ GitHub 자동 Push 함수 (추가)
# ===========================
def git_commit_and_push(search_query, current_page):
    try:
        # Git 사용자 설정
        subprocess.run(["git", "config", "--local", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
        subprocess.run(["git", "config", "--local", "user.name", "github-actions[bot]"], check=False)
        
        # 파일 추적 (새로 생성된 csv 및 json 모두 포함)
        subprocess.run(["git", "add", "*.csv", PROGRESS_FILE], check=True)
        
        # 변경사항이 있는지 확인 후 푸시
        status = subprocess.run(["git", "diff", "--cached", "--quiet"], check=False)
        if status.returncode != 0:
            commit_msg = f"Auto-update: '{search_query}' {current_page}페이지 완료 안전 저장"
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            # 💡 [핵심 추가] Push 하기 전에 GitHub 서버의 최신 상태를 먼저 당겨와서(Pull) 병합합니다.
            subprocess.run(["git", "pull", "--rebase"], check=False)
            subprocess.run(["git", "push"], check=True)
            logging.info(f"☁️ GitHub 자동 푸시 완료: {current_page}페이지 데이터 박제 성공!")
        else:
            logging.info(f"☁️ GitHub 저장: 변경된 데이터가 없습니다.")
    except Exception as e:
        logging.error(f"❌ GitHub 푸시 에러 (크롤링은 계속 진행): {e}")


def check_out_of_memory(driver):
    try:
        page_text = driver.find_element(By.TAG_NAME, 'body').text
        return "Out of Memory" in page_text or "앗, 이런!" in page_text
    except Exception: return False

def check_driver_alive(driver):
    try:
        driver.current_url
        watchdog.reset()
        return True
    except WebDriverException:
        logging.error("❌ WebDriver 연결 끊김 감지")
        return False
    except Exception as e: return False

def parse_operating_hours(hours_details):
    """영업시간 텍스트를 요일별 시작/종료 시간으로 정형화"""
    days = ['월', '화', '수', '목', '금', '토', '일']
    parsed_hours = {day: {"start": "정보 없음", "end": "정보 없음"} for day in days}
    
    if not hours_details or hours_details == "정보 없음": 
        return parsed_hours
        
    for item in hours_details:
        time_match = re.search(r'(\d{2}:\d{2})\s*-\s*((?:다음\s*날\s*)?\d{2}:\d{2})', item)
        is_closed = "휴무" in item
        
        if "매일" in item.split('\n')[0]:
            for day in days:
                if time_match:
                    parsed_hours[day]["start"] = time_match.group(1)
                    parsed_hours[day]["end"] = time_match.group(2).strip()
                elif is_closed:
                    parsed_hours[day]["start"] = "휴무"
                    parsed_hours[day]["end"] = "휴무"
            continue 
            
        day_match = re.search(r'([월화수목금토일])', item.split('\n')[0])
        if day_match:
            day = day_match.group(1)
            if time_match:
                parsed_hours[day]["start"] = time_match.group(1)
                parsed_hours[day]["end"] = time_match.group(2).strip()
            elif is_closed:
                parsed_hours[day]["start"] = "휴무"
                parsed_hours[day]["end"] = "휴무"
            
    return parsed_hours

# ===========================
# 🚀 웹 드라이버 설정 (V23 안정성 통합)
# ===========================
def initialize_driver():
    logging.info("🚀 ChromeDriver 초기화 중...")
    watchdog.reset()
    opts = Options()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer") 
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-features=VizDisplayCompositor") 
    opts.add_argument("--force-color-profile=srgb")
    opts.add_argument("--lang=ko_KR")
    opts.add_argument(f"user-agent={USER_AGENT}")
    opts.add_argument("--log-level=3") 
    opts.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    opts.add_experimental_option("useAutomationExtension", False)
    if HEADLESS_MODE: opts.add_argument("--headless=new")
    
    driver = webdriver.Chrome(options=opts, service=Service())
    driver.set_window_size(1440, 900)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    return driver

def safe_execute(func, *args, **kwargs):
    try:
        res = func(*args, **kwargs)
        watchdog.reset()
        return res
    except (TimeoutException, WebDriverException) as e:
        logging.error(f"⚠️ 연결/타임아웃 오류 감지: {type(e).__name__}")
        raise
    except Exception as e:
        logging.error(f"❌ 실행 오류: {type(e).__name__}")
        raise

def switch_to_frame(driver, frame_id):
    WebDriverWait(driver, 15).until(EC.frame_to_be_available_and_switch_to_it((By.ID, frame_id)))
    time.sleep(2)

# ===========================
# 🎯 데이터 추출 (SNS, 편의시설, 이미지 통합)
# ===========================
def extract_social_links(driver):
    social_data = {"instagram": "", "blog": ""}
    try:
        selectors = [
            "a[href*='instagram.com']", "a[href*='blog.naver.com']", 
            "a[href*='tistory.com']", "div.place_section_content a[href]"
        ]
        found_links = set()
        for sel in selectors:
            try:
                for link in driver.find_elements(By.CSS_SELECTOR, sel):
                    if href := link.get_attribute('href'): found_links.add(href)
            except Exception: continue
            
        for href in found_links:
            href_lower = href.lower()
            if 'instagram.com' in href_lower and not social_data["instagram"]:
                social_data["instagram"] = href.split('?')[0]
            elif not social_data["blog"] and ('blog.naver.com' in href_lower or 'tistory.com' in href_lower):
                social_data["blog"] = href.split('?')[0]
    except Exception: pass
    return social_data

def extract_data(driver, litag, log_idx=""):
    if check_out_of_memory(driver):
        raise WebDriverException("Out of Memory detected")

    try:
        litag.find_element(By.CSS_SELECTOR, "path.place_ad_label_border")
        return None  # 광고 건너뛰기
    except NoSuchElementException: pass

    try:
        litag.find_element(By.CSS_SELECTOR, "span.TYaxT").click()
        time.sleep(random.uniform(2, 3))
        driver.switch_to.default_content()
        switch_to_frame(driver, "entryIframe")
        watchdog.reset()

        # 1. 기본 정보 추출
        try: name = driver.find_element(By.CSS_SELECTOR, "span.GHAhO").text
        except Exception: name = "이름 없음"

        try: category = driver.find_element(By.CSS_SELECTOR, "span.lnJFt").text.strip()
        except Exception: category = "카테고리 없음"

        try: address = driver.find_element(By.CSS_SELECTOR, "span.LDgIH").text 
        except Exception: 
            try: address = driver.find_element(By.CSS_SELECTOR, "span.pz7wy").text 
            except Exception: address = "주소 없음"

        try: phone_number = driver.find_element(By.CSS_SELECTOR, "span.xlx7Q").text
        except Exception: phone_number = "전화번호 없음"

        try:
            rating = "평점 없음"
            for el in driver.find_elements(By.CSS_SELECTOR, "span.PXMot"):
                if "별점" in el.text:
                    rating = el.text.replace("별점", "").replace("\n", "").strip()
                    break
        except Exception: rating = "평점 없음"

        # 2. 영업시간 추출 및 정형화
        try:
            driver.find_element(By.CSS_SELECTOR, 'a.gKP9i.RMgN0').click()
            time.sleep(1)
            hours_details = [el.text for el in driver.find_elements(By.CSS_SELECTOR, "span.A_cdD")]
        except Exception: hours_details = "정보 없음"
        structured_hours = parse_operating_hours(hours_details)

        # 3. SNS 링크 추출
        social_links = extract_social_links(driver)

        # 4. 편의시설 및 주차 정보 추출
        facilities, parking = "", ""
        try:
            for xp in ["//span[normalize-space(.)='정보']/ancestor::a[@role='tab'][1]", "//a[@role='tab' and contains(., '정보')]"]:
                try:
                    info_tab = WebDriverWait(driver, 1.5).until(EC.presence_of_element_located((By.XPATH, xp)))
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", info_tab)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", info_tab)
                    time.sleep(1.5)
                    break
                except Exception: continue
            
            for sel in ["div.place_section.no_margin.xHqGd ul", "div.xHqGd ul.Uva5I"]:
                try: facilities = re.sub(r"\s+", " ", driver.find_element(By.CSS_SELECTOR, sel).text.strip()); break
                except Exception: pass
            
            for sel in ["div.place_section.no_margin.IrpYf > div", "div.place_section.no_margin.lrpYf > div"]:
                try: parking = re.sub(r"\s+", " ", driver.find_element(By.CSS_SELECTOR, sel).text.strip()); break
                except Exception: pass
        except Exception as e: logging.debug(f"정보 탭 수집 실패: {e}")

        # 5. 사진 탭 로직
        image_urls_str = "이미지 없음"
        try:
            image_urls = []
            for xp in ["//a[@role='tab' and contains(., '사진')]", "//a[contains(@class, '_tab-menu') and contains(., '사진')]"]:
                try:
                    photo_tab = WebDriverWait(driver, 1.5).until(EC.presence_of_element_located((By.XPATH, xp)))
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", photo_tab)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", photo_tab)
                    time.sleep(2)
                    break
                except Exception: continue
            
            try:
                business_tab = WebDriverWait(driver, 1.5).until(EC.presence_of_element_located((By.XPATH, "//a[@role='button' and contains(., '업체')]")))
                driver.execute_script("arguments[0].click();", business_tab)
                time.sleep(1.5)
            except Exception: pass 

            for i in range(2):
                try:
                    src = driver.find_element(By.ID, f"업체_{i}").get_attribute('src')
                    if src:
                        qs = urllib.parse.parse_qs(urllib.parse.urlparse(src).query)
                        image_urls.append(qs['src'][0] if 'src' in qs else src.split('?')[0])
                except Exception: continue
            
            if not image_urls:
                for img in driver.find_elements(By.CSS_SELECTOR, "div.place_thumb img"):
                    src = img.get_attribute('src')
                    if src and "pstatic.net" in src:
                        qs = urllib.parse.parse_qs(urllib.parse.urlparse(src).query)
                        clean_src = qs['src'][0] if 'src' in qs else src.split('?')[0]
                        if clean_src not in image_urls: image_urls.append(clean_src)
                    if len(image_urls) >= 2: break

            image_urls_str = ", ".join(image_urls) if image_urls else "이미지 없음"
        except Exception as e: logging.debug(f"사진 탭 수집 실패: {e}")

        watchdog.reset()
        
        return {
            "Name": name, "Category": category, "Address": address, "Phone": phone_number,
            "Rating": rating, "Facilities": facilities, "Parking": parking,
            "Images": image_urls_str,
            "Mon_Start": structured_hours["월"]["start"], "Mon_End": structured_hours["월"]["end"],
            "Tue_Start": structured_hours["화"]["start"], "Tue_End": structured_hours["화"]["end"],
            "Wed_Start": structured_hours["수"]["start"], "Wed_End": structured_hours["수"]["end"],
            "Thu_Start": structured_hours["목"]["start"], "Thu_End": structured_hours["목"]["end"],
            "Fri_Start": structured_hours["금"]["start"], "Fri_End": structured_hours["금"]["end"],
            "Sat_Start": structured_hours["토"]["start"], "Sat_End": structured_hours["토"]["end"],
            "Sun_Start": structured_hours["일"]["start"], "Sun_End": structured_hours["일"]["end"],
            "Instagram": social_links["instagram"], "Blog": social_links["blog"]
        }
    except Exception as e:
        logging.error(f"  ❌ [{log_idx}] 데이터 추출 실패: {e}")
        return None
    finally:
        driver.switch_to.default_content()
        switch_to_frame(driver, "searchIframe")
        time.sleep(random.uniform(2, 3))

# ===========================
# 🔄 메인 크롤링 로직
# ===========================
def crawl_search_query(driver, search_query, start_page=1):
    watchdog.start()
    try:
        driver.get("https://map.naver.com/v5/search")
        box = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.input_box>input.input_search")))
        box.clear()
        box.send_keys(search_query)
        box.send_keys(Keys.ENTER)
        time.sleep(random.uniform(4, 6))
        
        switch_to_frame(driver, "searchIframe")
        time.sleep(random.uniform(3, 5))
        
        current_page = start_page
        
        if start_page > 1:
            for _ in range(start_page - 1):
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, '#app-root > div > div.XUrfU > div.zRM9F > a:nth-child(7)')
                    if 'disabled' not in next_button.get_attribute('class'):
                        next_button.click()
                        time.sleep(random.uniform(6, 8))
                except Exception: break

        while current_page <= MAX_PAGE:
            if check_out_of_memory(driver) or not check_driver_alive(driver):
                raise WebDriverException("메모리 부족 또는 드라이버 끊김")

            data = []
            container = driver.find_element(By.CLASS_NAME, 'Ryr1F')
            last_height = driver.execute_script("return arguments[0].scrollHeight", container)
            while True:
                driver.execute_script("arguments[0].scrollTop += 1500;", container)
                time.sleep(random.uniform(2.0, 3.0))
                new_height = driver.execute_script("return arguments[0].scrollHeight", container)
                if new_height == last_height: break
                last_height = new_height

            litags = driver.find_elements(By.CSS_SELECTOR, '#_pcmap_list_scroll_container.Ryr1F>ul>li.UEzoS.rTjJo')
            logging.info(f"🪧 '{search_query}' {current_page}페이지: {len(litags)}개 항목 발견")
            
            consecutive_failures = 0
            for idx, litag in enumerate(litags, 1):
                try:
                    watchdog.reset()
                    if restaurant_info := safe_execute(extract_data, driver, litag, str(idx)):
                        data.append(restaurant_info)
                        
                        insta = "📷" if restaurant_info.get('Instagram') else ""
                        blog = "📝" if restaurant_info.get('Blog') else ""
                        logging.info(f"  ✓ {idx}: {restaurant_info['Name']} {insta}{blog}")
                        consecutive_failures = 0
                except Exception:
                    consecutive_failures += 1
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES: raise WebDriverException("연속 실패 임계점 도달")
            
            if data:
                safe_execute(save_data, data, f"{search_query}.csv")
            
            # --- 💡 수정된 핵심 로직: 페이지 완료 후 상태 저장, 깃허브 푸시, 재시작 신호 반환 ---
            if current_page >= MAX_PAGE:
                mark_search_completed(search_query) # JSON을 완료 상태로 덮어쓰기
                git_commit_and_push(search_query, current_page) # 깃허브에 영구 저장
                logging.info(f"✅ 검색어 '{search_query}' (최대 {MAX_PAGE}페이지) 크롤링 완료")
                return True
                
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, '#app-root > div > div.XUrfU > div.zRM9F > a:nth-child(7)')
                if 'disabled' in next_button.get_attribute('class'):
                    mark_search_completed(search_query)
                    git_commit_and_push(search_query, current_page)
                    logging.info(f"✅ 검색어 '{search_query}' (마지막 페이지) 크롤링 완료")
                    return True
                else:
                    save_progress(search_query, current_page + 1) # 다음 시작할 페이지 번호(+1) 저장
                    git_commit_and_push(search_query, current_page) # 지금까지의 결과 깃허브에 박제!
                    logging.info(f"🧹 메모리 최적화: {current_page}페이지 완료. 브라우저를 선제적으로 재시작합니다.")
                    return "RESTART_REQUIRED" # 계속 돌지 말고 멈추라는 신호 전송
            except Exception:
                mark_search_completed(search_query)
                git_commit_and_push(search_query, current_page)
                logging.info(f"✅ 검색어 '{search_query}' 크롤링 완료 (다음 버튼 없음)")
                return True
            # --------------------------------------------------------

    finally:
        watchdog.stop()

# ===========================
# 🏁 진입점
# ===========================
def main():
    driver = None
    try:
        if len(sys.argv) > 1: 
            search_terms = [k.strip() for k in sys.argv[1].split(',') if k.strip()]
        else:
            try: 
                search_terms = pd.read_csv(SEARCH_FILE)['검색어'].tolist()
            except Exception as e: 
                return logging.error(f"❌ {SEARCH_FILE} 파일을 찾을 수 없거나 열 수 없습니다: {e}")

        logging.info(f"📋 총 {len(search_terms)}개 검색어 로드 완료: {search_terms}")

        progress = load_progress()
        completed = set(progress.get("completed_searches", []))
        driver = initialize_driver()

        # 💡 [추가] 오늘 처리한 검색어 개수를 세는 변수
        processed_count = 0
        
        for search_query in search_terms:
            if search_query in completed: 
                logging.info(f"⭐️ '{search_query}' 이미 완료됨, 건너뜀")
                continue

            # 💡 [추가] 2개를 이미 처리했다면 다음 검색어로 넘어가지 않고 완전히 종료!
            if processed_count >= 2:
                logging.info("🛑 목표한 검색어 2개 처리를 모두 완료했습니다. 다음 스케줄에 이어서 진행합니다.")
                break
            
            # 새로운 검색어 처리를 시작하므로 카운트 1 증가
            processed_count += 1
            
            start_page = progress.get("current_page", 1) if progress.get("current_search") == search_query else 1
            retry_count = 0
            
            while retry_count < MAX_RETRY_ATTEMPTS:
                try:
                    save_progress(search_query, start_page)
                    result = crawl_search_query(driver, search_query, start_page)
                    
                    if result is True:
                        break # 모든 크롤링이 정상적으로 진짜 완료되었을 때만 탈출
                    elif result == "RESTART_REQUIRED":
                        # 💡 1페이지 정상 완료 후 크롬을 끄고 다음 페이지를 위해 다시 켜는 로직
                        try: driver.quit()
                        except Exception: pass
                        time.sleep(8) # 네이버 봇 차단을 피하기 위해 8초 대기
                        driver = initialize_driver()
                        start_page = load_progress().get("current_page", 1) # 저장해둔 다음 페이지 번호 불러오기
                        continue # retry_count를 올리지 않고 안전하게 계속 진행
                        
                except Exception as e:
                    retry_count += 1
                    logging.error(f"⚠️ 오류 발생 (재시도 {retry_count}/{MAX_RETRY_ATTEMPTS}): {e}")
                    if retry_count < MAX_RETRY_ATTEMPTS:
                        try: driver.quit()
                        except Exception: pass
                        time.sleep(10)
                        driver = initialize_driver()
                        start_page = load_progress().get("current_page", 1)
                    else: break
    except KeyboardInterrupt:
        # 정상적으로 사용자의 Ctrl+C 중단을 인지하고 즉시 종료!
        logging.info("⚠️ 사용자에 의해 안전하게 중단되었습니다. (다음 실행 시 이어서 시작됩니다.)")
    finally:
        if driver:
            try: driver.quit()
            except Exception: pass

if __name__ == "__main__":
    logging.info("="*60)
    logging.info("🚀 네이버 지도 통합 크롤러 (안전 중단 기능 보완)")
    logging.info("="*60)
    main()
