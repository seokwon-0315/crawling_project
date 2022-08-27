import pandas as pd

# url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev?type=xml&"
# key = "thdbhvmVO6egpIpWZonZgS4M0TnMMNe1Q%2Bn6WQwBEqSYYtZgqyr%2Fp3ixWNn3YqVfcSYDBgl3Lg1tf8aGhzYSfw%3D%3D"
url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev?_wadl&type=xml"
key = "thdbhvmVO6egpIpWZonZgS4M0TnMMNe1Q%2Bn6WQwBEqSYYtZgqyr%2Fp3ixWNn3YqVfcSYDBgl3Lg1tf8aGhzYSfw%3D%3D"

APT_DETAIL_ENDPOINT = f"{url}serviceKey={key}&"

state_info = pd.read_csv('state_info.csv')
state_info = state_info.query("state == '서울특별시' & district != ' '")
# state_info = state_info.query("district != ' '")
state_info = state_info[3:]