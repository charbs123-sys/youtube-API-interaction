import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

df = pd.read_csv('from_mysql.csv', sep = ';', encoding='cp1252')

#remove video_id col, removing title for now
df = df.drop(columns=['vid_id','title', 'fav_count'])


#1 - view count is a time series, we can try to implement ARMA model and the likes 

#2 - simply implement linear regression 
linear = LinearRegression()
X = df[['like_count','com_count']]
y = df[['view_count']]
reg = linear.fit(X, y)
#3 - use title, determine key words affecting view_count
