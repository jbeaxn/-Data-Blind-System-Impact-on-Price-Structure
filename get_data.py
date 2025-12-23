import requests
import pandas as pd
from datetime import datetime, timedelta
import time

#wsl이 아니라 cmd로

SERVICE_KEY = "a844930b63c50e250d3af6608359d37aeef4436a1e222afca47aa4847a3b91da"

URL = "https://apis.data.go.kr/B552845/katOrigin/trades"

market_code = "210001"   # 시장코드
gds_lclsf_cd = "06"       # 대분류
gds_mclsf_cd = "01"       # 중분류

def get_trades(date):
    params = {
    "serviceKey": SERVICE_KEY,
    "returnType": "json",
    "cond[trd_clcln_ymd::EQ]": date,         # 날짜
    "cond[whsl_mrkt_cd::EQ]": market_code,   # 시장명
    "cond[gds_lclsf_cd::EQ]": gds_lclsf_cd, # 대분류 코드
    "cond[gds_mclsf_cd::EQ]": gds_mclsf_cd, # 중분류 코드
    "numOfRows": 3000,     # 한 페이지 최대 데이터 수
    "pageNo": 1
}


    try:
        r = requests.get(URL, params=params, timeout=30) 

        # HTTP 오류 체크 
        if r.status_code != 200:
            print(f"HTTP 오류: {r.status_code}, 응답: {r.text[:200]}...")
            return []

        data = r.json()

        # 공공데이터포털 API 오류 체크
        if "response" in data and "header" in data["response"]:
            header = data["response"]["header"]
            
            # "00"이 아닌 "0"을 정상 코드로 간주 (str로 형변환하여 0과 "0" 모두 처리)
            result_code_str = str(header["resultCode"])

            if result_code_str != "0": # "0"이 아니면 오류로 간주
                if result_code_str == "4" or result_code_str == "04": # NO_DATA
                    return [] 
                print(f"API 오류: {header['resultMsg']} (코드: {header['resultCode']})")
                return []
            
        # 데이터가 정상적으로 있는지 확인
        if "response" not in data or "body" not in data["response"] or "items" not in data["response"]["body"]:
             print(f"API 응답 형식 오류: 'response/body/items' 구조가 없습니다. 응답: {str(data)[:200]}...")
             return []

        items_data = data["response"]["body"]["items"]

        # 그날 거래가 없으면 빈 리스트 반환 (오류 아님)
        if not items_data or "item" not in items_data or not items_data["item"]:
            return [] 

        items = items_data["item"]

        # 데이터가 1건 or 여러건 처리
        if isinstance(items, dict):
            return [items]  # 1건의 데이터도 리스트에 담아 반환
        elif isinstance(items, list):
            return items    # 여러 건이면 리스트 그대로 반환
        else:
            return []       # 그 외의 경우는 빈 리스트 반환

    # 예외 처리
    except requests.exceptions.JSONDecodeError:
        print(f"JSON 파싱 오류. 서버가 JSON이 아닌 응답을 보냈습니다. 응답: {r.text[:200]}...")
        return []
    except requests.exceptions.RequestException as e:
        print(f" 네트워크/요청 오류: {e}")
        return []
    except KeyError as e:
        print(f"JSON 구조 오류: 키 '{e}'를 찾을 수 없습니다.")
        print(f"   수신한 데이터: {str(data)[:200]}...")
        return []
    except Exception as e:
        print(f"알 수 없는 오류: {e}")
        return []


def collect_year(year):

    print("=" * 60)
    print(f"📌 {year}년 대구북부시장 수집 시작")

    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)

    all_rows = []
    cur = start

    while cur <= end:
        date_str = cur.strftime("%Y-%m-%d")
        print(f"📅 {date_str} 조회 중...")

        rows = get_trades(date_str)

        if rows:
            print(f"  ➜ {len(rows)}건 수집")
            for r in rows:
                r["date"] = date_str
            all_rows.extend(rows)

        cur += timedelta(days=1)
        time.sleep(0.15)  # API 부하 방지

    if not all_rows:
        print("!! 연도 전체 데이터 없음")
        return

    df = pd.DataFrame(all_rows)
    # 컴파일 전 꼭, 파일이름 변경 확인
    filename = f"대구북부시장_사과전체_{year}.xlsx"
    df.to_excel(filename, index=False)
    print(f"!! 저장 완료 → {filename}")


# ----------------------
# 연도 입력해서 각 추출
# ----------------------
collect_year(2025)
