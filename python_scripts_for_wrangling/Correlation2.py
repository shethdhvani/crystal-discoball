from scipy.stats import linregress
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import math

//Author Aditya ,Dhvani Apoorv

df = pd.read_csv('C:/Users/Aditya/Desktop/project/features/all_features11.csv')
#print(df)
df_filtered = (df.query('td_confidence>1 '))
downloads = df_filtered.iloc[:,16].values

artist_hotness = df_filtered.iloc[:,1].values
artist_familarity =df_filtered.iloc[:,2].values
loudness = df_filtered.iloc[:,3].values
tempo = df_filtered.iloc[:,4].values
duration = df_filtered.iloc[:,5].values
song_hotness = df_filtered.iloc[:,6].values
year = df_filtered.iloc[:,7].values
play_count = df_filtered.iloc[:,8].values
song_count = df_filtered.iloc[:,9].values
weight_dot_freq = df_filtered.iloc[:,10].values
similar_artists = df_filtered.iloc[:,11].values
likes = df_filtered.iloc[:,12].values
td_meanprice = df_filtered.iloc[:,13].values
td_confidence = df_filtered.iloc[:,0].values
top_genere_match = df_filtered.iloc[:,14].values
popularity = df_filtered.iloc[:,15].values

def printMeanMedia(arr,s):

    print("Mean Median of "+ s)
    print(np.mean(arr))
    print(np.median(arr))


printMeanMedia(artist_hotness,"Artist Hotness")
printMeanMedia(artist_familarity,"Artist Familiarity")
printMeanMedia(loudness,"Loudness")
printMeanMedia(tempo,"Tempo")
printMeanMedia(duration,"Duration")
printMeanMedia(song_hotness,"Song Hotness")
printMeanMedia(year,"year")
printMeanMedia(play_count,"play count")
printMeanMedia(song_count,"song count")
printMeanMedia(weight_dot_freq,"Weight dot Freq")
printMeanMedia(similar_artists,"Similar Artists")
printMeanMedia(likes,"Likes")
printMeanMedia(td_meanprice,"Mean Price")
printMeanMedia(td_confidence,"Confidence")
printMeanMedia(top_genere_match,"Genre Match")
printMeanMedia(popularity,"Popularity")



dot = []
for i in range(len(similar_artists)):
    dot.append(similar_artists[i]*artist_familarity[i]*song_count[i])

dot = list(map(lambda x:math.log(x+1),dot))
popularity =dot

#print(len(popularity))
print("Artist hotness")
c1 = linregress(downloads,artist_hotness)
print(c1.rvalue)
print("Artist Familiarity")
c2 = linregress(downloads,artist_familarity)
print(c2.rvalue)
print("Loudness")
c3 = linregress(downloads,loudness)
print(c3.rvalue)
print("Tempo")
c4 = linregress(downloads,tempo)
print(c4.rvalue)
print("Duration")
c5 = linregress(downloads,duration)
print(c5.rvalue)
print("Song Hotness")
c6 = linregress(downloads,song_hotness)
print(c6.rvalue)
print("year")
c7 = linregress(downloads,year)
print(c7.rvalue)
print("play_count")
c8 = linregress(downloads,play_count)
print(c8.rvalue)
print("song_count")
c9 = linregress(downloads,song_count)
print(c9.rvalue)
print("weight_dot_freq")
c10 = linregress(downloads,weight_dot_freq)
print(c10.rvalue)
print("similar_artists")
c11 = linregress(downloads,similar_artists)
print(c11.rvalue)
print("likes")
c12 = linregress(downloads,likes)
print(c12.rvalue)
print("Mean Price")
c13 = linregress(downloads,td_meanprice)
print("Confidence")
print(c13.rvalue)
c14 = linregress(downloads,td_confidence)
print(c14.rvalue)
print("Genre Match")
c16 = linregress(downloads,top_genere_match)
print(c16.rvalue)
print("Popularity")
c17 = linregress(downloads,popularity)
print(c17.rvalue)
#print(td_meanprice.sum())

from sklearn.cluster import KMeans
km = KMeans()
km.fit(downloads.reshape(-1,1))
print(km.cluster_centers_)


plt.scatter(popularity, downloads)
plt.title("Popularity vs downloads")
plt.xlabel("Popularity")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(artist_hotness, downloads)
plt.title("Artist_hotness vs downloads")
plt.xlabel("Artist_hotness")
plt.ylabel("Downloads")
plt.show()
# #
plt.scatter(artist_familarity,downloads)
plt.title("Artist Familiarty vs downloads")
plt.xlabel("Artist Familiarity")
plt.ylabel("Downloads")
plt.show()
# #
plt.scatter(loudness,downloads)
plt.title("Loudness vs downloads")
plt.xlabel("Loudness")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(tempo,downloads)
plt.title("Tempo vs downloads")
plt.xlabel("Tempo")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(duration,downloads)
plt.title("Duration vs downloads")
plt.xlabel("Duration")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(song_hotness,downloads)
plt.title("Song hotness vs downloads")
plt.xlabel("Song hotness")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(year,downloads)
plt.title("year vs downloads")
plt.xlabel("year")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(play_count,downloads)
plt.title("Play count vs downloads")
plt.xlabel("Play count")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(song_count,downloads)
plt.title("Song count vs downloads")
plt.xlabel("Song count")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(weight_dot_freq,downloads)
plt.title("Term Weight dot Freq vs downloads")
plt.xlabel("Term Weight dot Freq")
plt.ylabel("Downloads")
plt.show()
#
plt.scatter(similar_artists,downloads)
plt.title("Similar Artists vs downloads")
plt.xlabel("Similar Artists")
plt.ylabel("Downloads")
plt.show()

plt.scatter(likes,downloads)
plt.title("Likes vs downloads")
plt.xlabel("Likes")
plt.ylabel("Downloads")
plt.show()

plt.scatter(td_meanprice,downloads)
plt.title("Mean Price vs downloads")
plt.xlabel("Mean Price")
plt.ylabel("Downloads")
plt.show()


# all_data = [np.random.normal(0, std, 10) for std in range(6, 10)]
# print(all_data)

plt.scatter(td_confidence,downloads)
plt.title("Confidence vs downloads")
plt.xlabel("Confidence")
plt.ylabel("Downloads")
plt.show()

# plt.scatter(dot,downloads)
# plt.title("Popularity vs downloads")
# plt.xlabel("Popularity")
# plt.ylabel("Downloads")
# plt.show()

plt.scatter(top_genere_match,downloads)
plt.title("genre match vs downloads")
plt.xlabel("Genere Match")
plt.ylabel("Downloads")
plt.show()
#
# # data = []
# # for i in range(len(downloads)):
# #     data.append([downloads[i],td_confidence[i]])
# #     print(downloads[i],td_confidence[i])
# #     if i> 100:
# #         break
# # data = np.asarray(data)
# #print(df[df.td_confidence == 5]['td_downloads'].count())
# # print(df[df.td_confidence == 2]['td_downloads'])
# # print(df[df.td_confidence == 3]['td_downloads'])
# # print(df[df.td_confidence == 4]['td_downloads'])
# # print(df[df.td_confidence == 5]['td_downloads'])
# # dataSet = [
# #     df[df.td_confidence == 2]['td_downloads'],
# #     df[df.td_confidence == 3]['td_downloads'],
# #     df[df.td_confidence == 4]['td_downloads'],
# #     df[df.td_confidence == 5]['td_downloads']]
# # xticklabels = ['0','Average', 'Good', 'very good', 'Excellent']
# #
# #
# # plt.violinplot(dataSet)
# # fig = plt.figure(1)
# # ax = fig.add_subplot(111)
# # plt.xticks(range(0,5))
# # ax.set_xticklabels(xticklabels)
# # plt.setp(ax)
# # plt.ylabel("Downloads")
# # plt.xlabel("Confidence")
# # plt.title("Confidence vs Downloads")
# # plt.show()





