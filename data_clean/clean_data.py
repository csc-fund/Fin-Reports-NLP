import re
# -----------------------数据清洗-----------------------#
def delete_tag(s):
    r1 = re.compile(r'\{IMG:.?.?.?\}')  # 图片
    s = re.sub(r1, '', s)
    r2 = re.compile(r'[a-zA-Z]+://[^\u4e00-\u9fa5|\?]+')  # 网址
    s = re.sub(r2, '', s)
    r3 = re.compile(r'<.*?>')  # 网页标签
    s = re.sub(r3, '', s)
    r4 = re.compile(r'&[a-zA-Z0-9]{1,4}')  # &nbsp  &gt  &type &rdqu   ....
    s = re.sub(r4, '', s)
    r5 = re.compile(r'[0-9a-zA-Z]+@[0-9a-zA-Z]+')  # 邮箱
    s = re.sub(r5, '', s)
    r6 = re.compile(r'[#]')  # #号
    s = re.sub(r6, '', s)
    return s


train['title'] = train['title'].apply(lambda x: delete_tag(x) if str(x) != 'nan' else x)
train['text'] = train['text'].apply(lambda x: delete_tag(x) if str(x) != 'nan' else x)
test['title'] = test['title'].apply(lambda x: delete_tag(x) if str(x) != 'nan' else x)
test['text'] = test['text'].apply(lambda x: delete_tag(x) if str(x) != 'nan' else x)



# -----------------------数据清洗-----------------------#

