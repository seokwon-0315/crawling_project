import pandas as pd

data = pd.read_csv('../법정동코드 전체자료.txt', sep = "\t", engine='python', encoding = "cp949")
data2 = data.query("폐지여부 == '존재'")[['법정동코드','법정동명']]
data2 = data2.rename(columns={'법정동코드':'code','법정동명':'name'})
data2['code2'] = data2['code'].apply(lambda x : int(str(x)[:5]))

# 전체 주소를 공백으로 잘라서 일부만 가져오는 함수 생성
def get_state(x, idx):
    name_splited = x.split(' ')
    if idx == 0:
        return name_splited[idx]
    else :
        if len(name_splited) >= idx+1:
            return name_splited[idx]
        else:
            return ''

# 전체 주소의 처음을 state로 저장
data2['state'] = data2['name'].apply(lambda x : get_state(x,0))
# 전체 주소의 두번째를 district로 저장
data2['district'] = data2['name'].apply(lambda x : get_state(x,1))

data3 = data2[['code2', 'state', 'district']]
data3 = data3.rename(columns={'code2':'code'})
data3.drop_duplicates().to_csv('state_info.csv', index=False)
