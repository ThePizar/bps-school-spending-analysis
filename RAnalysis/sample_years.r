avgs<-read.csv('averages.csv')
years<-read.csv('years.csv')
years<-years[years$Year < 2017,]
require(ggplot2)

data=years[years$School == "Brighton High School",]

#One School
brighton <- ggplot(data=years[years$School == "Brighton High School",], aes(x=Year, y=Total,fill = School)) + geom_bar(stat = "identity")

brighton

#second School
bls <- ggplot(data=years[years$School == "Boston Latin School",], aes(x=Year, y=Total,fill = School)) + geom_bar(stat = "identity")

bls

#Two School
#twoBase <- ggplot(data=years[years$School == "Brighton High School" | years$School == "Boston Latin School",], aes(x=Year, y=Total, fill = School))

#twoBase + geom_bar(stat = "identity", position = "dodge")
