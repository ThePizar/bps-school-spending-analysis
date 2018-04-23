
descs<-read.csv('normal.csv')
descs<-descs[descs]

years<-read.csv('years.csv')

require(ggplot2)

schools <- unique(descs$School)

for(school in schools) {
  fixedName <- sub("/", " ", school)
  name <- paste(fixedName, ".png", sep = "")
  cats <- ggplot(data=descs[descs$School == school,], aes(x=AccountDesc, y=Amount)) + geom_bar(stat = "identity",fill="steelblue2") + coord_flip()
  cats <- cats + labs(title = "Normalized Spending by Category", y = "Spending Compared to Average School Spending", x = "Spending Category") 
  ggsave(name, plot = cats, path = "output/cats")
  history <- ggplot(data=years[years$School == school,], aes(x=Year, y=Total)) + geom_area(fill="steelblue2")
  history <- history + labs(title = "Spending over Years", x="Years", y="Spending")
  ggsave(name, plot = history, path = "output/history")
}

#ggplot(data=descs[descs$School == "Brighton High School",], aes(x=AccountDesc, y=Amount,fill = School)) + geom_bar(stat = "identity") + coord_flip()

#ggplot(data=descs[descs$School == "Boston Latin School",], aes(x=AccountDesc, y=Amount,fill = School)) + geom_bar(stat = "identity") + coord_flip()

