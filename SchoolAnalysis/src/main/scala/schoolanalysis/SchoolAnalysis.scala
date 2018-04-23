package schoolanalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class CheckBookEntry(voucher: String, voucherLine: Int, distributionLine: Int,
                          entered: String, monthNum: Int, fiscalMonth: Int, month: String,
                          year: Int, fiscalYear: Int, vendorName: String,
                          account: Int, accountDesc: String, dept: String, deptName: String,
                          program: String, amount: Double)

case class BPSInfo(school: String, address: String, latitude: Double, longitude: Double, students: Int,
                   program: String, grades: String)

case class AvgCosts(school: String, students: Int, total: Double, perStudent: Int, perPayment: Int, perYear: Int)

case class MonthCosts(school: String, year: Int, month: Int, cost: Double)

case class YearCosts(school: String, year: Int, cost: Double)

case class SimpleEntry(school: String, accountDesc: String, year: Int, month: Int, amount: Double)

case class DescCosts(school: String, accountDesc: String, amount: Double)

object SchoolAnalysis {

  //From: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sparkcontext-creating-instance-internals.html
  // 1. Create Spark configuration
  val conf : SparkConf = new SparkConf()
    .setAppName("School Anaylsis")
    .setMaster("local[*]")  // local mode

  // 2. Create Spark context
  val sc = new SparkContext(conf)

  val ss : SparkSession = SparkSession.builder().config(conf).getOrCreate()

  import ss.implicits._

  val checkbook : RDD[String] = sc.textFile("../../checkbook-explorer.csv") //Has slight fixes
  val bpsinfo : RDD[String] = sc.textFile("../../buildbps.csv") //Has slight fixes

  def splitRow(str: String) : Array[String] = {
    val iter = str.split(",").reverseIterator
    if (!iter.hasNext) return Array()
    var curr = iter.next()
    var list = List[String]()
    while (iter.hasNext) {
      if (curr.nonEmpty && curr.last == '\"' && curr.head != '\"') {
        curr = iter.next() + "," + curr
      }
      else {
        list = curr :: list
        curr = iter.next()
      }
    }
    list = curr :: list
    list.toArray.map(item => {
      if (item.length > 1 && item.last == '\"' && item.head == '\"') item.substring(1, item.length - 1)
      else item
    })
  }

  def join (arr: Array[String], first: Int, last: Int, glue: String): String = {
    arr.slice(first, last).reduce((acc, curr) => {
      acc + "," + curr
    })
  }

  def classifyCheckBook(rDD: RDD[String]) : RDD[CheckBookEntry] = {
    rDD.map(row => {
      val split = splitRow(row)
      val commas = split.length
      val expected = 16
      val extra = commas - expected// - deptNameFactor - programFactor
      CheckBookEntry(
        voucher = split(0),
        voucherLine = split(1).toInt,
        distributionLine = split(2).toInt,
        entered = split(3),
        monthNum = split(4).toInt,
        fiscalMonth = split(5).toInt,
        month = split(6),
        year = split(7).toInt,
        fiscalYear = split(8).toInt,
        vendorName = join(split, 9, 9 + extra + 1, ","),
        account = split(10 + extra).toInt,
        accountDesc = split(11 + extra),
        dept = split(12 + extra),
        deptName = split(13),
        program = split(14),
        amount = split(15 + extra).stripPrefix("$").toDouble
      )
    })
  }

  def classifyBPS(rDD: RDD[String]) : RDD[BPSInfo] = {
    rDD.flatMap(row => {
      val split = splitRow(row)
      if (split.length <= 30) None
      else
      Some(BPSInfo(
        school = split(2),
        address = split(5),
        latitude = split(7).toDouble,
        longitude = split(8).toDouble,
        students = split(26).toInt,
        program = split(9),
        grades = split(10)
      ))
    })
  }

  def checkCheckBook () : Unit = {
    val entries = classifyCheckBook(checkbook.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)).persist()
    val bpsEntries = entries.filter(entry => {
      entry.deptName == "Boston Public School Dept"
    }).persist()

    val bpsGrouped = bpsEntries.map(entry => {
      (entry.program, (1, entry.amount))
    }).reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    }).mapValues(pair => (pair._1, Math.round(pair._2))).sortByKey().collect()

    val YMGrouped = entries.map(entry => {
      ((entry.monthNum, entry.year), 1)
    }).reduceByKey(_+_).sortByKey().collect()

    val total = bpsEntries.count()

    val costBreakdown = bpsEntries.map(entry => {
      (Math.floor(Math.log10(entry.amount)), 1)
    }).reduceByKey(_+_).sortByKey().collect()

    val noCost = bpsEntries.filter(entry => {
      entry.amount == 0
    }).count()

    val smolCost = bpsEntries.filter(entry => {
      entry.amount > 0 && entry.amount < 1
    }).count()

    println("Counts:")
    bpsGrouped.foreach(tuple => {
      println(tuple._1 + " " + tuple._2)
    })
    println("Total entries:")
    println(total)

    println("Y/M pairs")
    YMGrouped.foreach(ym => {
      println(ym._1._1 + " " + ym._1._2 + ": " + ym._2)
    })

    println("Costs pairs")
    costBreakdown.foreach(cost => {
      println(cost._1 + ": " + cost._2)
    })

    println("No costs")
    println(noCost)

    println("Low costs")
    println(smolCost)
  }

  def checkBPS () : Unit = {
    val buildEntries = classifyBPS(bpsinfo.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)).persist()

    val firsts = buildEntries.take(10)

    firsts.foreach(info => {
      println(info)
    })
  }

  def mergeData (schoolsCheckBook : RDD[CheckBookEntry], bPSInfo: RDD[BPSInfo]) : RDD[(String, (CheckBookEntry, BPSInfo))] = {
    val schoolsMarked = schoolsCheckBook.map(entry => {
      (entry.program, entry)
    })
    val bpsMarked = bPSInfo.map(info => {
      (info.school, info)
    })
    schoolsMarked.join(bpsMarked)
  }

  def mapCheckBook (schoolCheckBook: RDD[CheckBookEntry], schools: Array[BPSInfo]): RDD[(BPSInfo, CheckBookEntry)] = {
    schoolCheckBook.filter(entry => {
      val banned = Array("AcadSup", "BPS", "Superintendent", "Officer")
      val hasBanned = banned.exists(str => {
        entry.program.contains(str)
      })
      entry.deptName == "Boston Public School Dept" && !hasBanned
    }).flatMap(entry => {
      val idx = getCorrectName(entry.program)
      if (idx >= 0) Some((schools(idx), entry))
      else None
    })
  }

  //case class AvgCosts(school: String, perStudent: Int, perPayment: Int, perYear: Int)
  def getAvgCost (info: RDD[(BPSInfo, CheckBookEntry)]) : RDD[AvgCosts] = {
    info.groupByKey().map(school => {
      val (info, list) = school
      val total : Double = list.foldRight(0.0)(_.amount + _)
      AvgCosts(
        school = info.school,
        students = info.students,
        total = total,
        perStudent = (total/info.students).round.toInt,
        perPayment = (total/list.size).round.toInt,
        perYear = (total/list.map(_.year).toList.distinct.size).round.toInt
      )
    })
  }

  def getMonthCosts(info: RDD[(BPSInfo, CheckBookEntry)]) : RDD[MonthCosts] = {
    info.groupByKey().flatMap(school => {
      val (info, list) = school
      list.groupBy(entry => (entry.year, entry.monthNum)).mapValues(_.foldRight(0.0)(_.amount + _)).map(month => {
        MonthCosts(
          school = info.school,
          year = month._1._1,
          month = month._1._2,
          cost = month._2
        )
      })
    })
  }

  def getYearCosts(info: RDD[(BPSInfo, CheckBookEntry)]) : RDD[YearCosts] = {
    info.groupByKey().flatMap(school => {
      val (info, list) = school
      list.groupBy(_.year).mapValues(_.foldRight(0.0)(_.amount + _)).map(year => {
        YearCosts(
          school = info.school,
          year = year._1,
          cost = year._2
        )
      })
    })
  }

  def getSimpleCosts(info: RDD[(BPSInfo, CheckBookEntry)]) : RDD[SimpleEntry] = {
    info.map(row => {
      val (school, entry) = row
      SimpleEntry(
        school = school.school,
        accountDesc = entry.accountDesc,
        year = entry.year,
        month = entry.monthNum,
        amount = entry.amount
      )
    })
  }

  def getDescCostsAvg(info: RDD[(BPSInfo, CheckBookEntry)]) : RDD[DescCosts] = {
    info.groupByKey().flatMap({case (info, list) =>
        list.groupBy(_.accountDesc).mapValues(_.foldRight(0.0)({case (entry, acc) =>
            acc + entry.amount
        })).toList.map({case (account, amount) =>
            DescCosts(
              school = info.school,
              accountDesc = account,
              amount = amount
            )
        })
    })
  }

  def normalizeDescCosts(costs : RDD[DescCosts]) : RDD[DescCosts] = {
    val byAcc = costs.groupBy(_.accountDesc).persist()
    val avgs = byAcc.mapValues(grouped => {
      grouped.foldRight(0.0)(_.amount + _) / grouped.size
    })
    byAcc.join(avgs).map({case (acc, (list, avg)) => list.map(d => {
      d.copy(amount = d.amount / avg)
    })}).filter(list => {list.size > 5 && !list.exists(_.amount > 10)}).flatMap(list => list) //y u no _
  }

  //Manual clean
  val schoolMap : Map[String, Int] = Map(
    "Adams Elementary School" -> 0,
    //Agassiz Elementary School?
    "Alighieri Montessori Schooll" -> 1,
    //Another Course to College not in Checkbook
    "Baldwin ELC" -> 3,
    "Bates Elementary School" -> 4,
    "Beethoven Elementary Schoo" -> 5,
    "Blackstone Elementary Scho" -> 6,
    "Boston Adult Technical Academy" -> 7,
    "Boston Arts-Pilot High School" -> 8,
    "Boston Community Leadrshp Acad" -> 9,
    //"Boston International High Schl" -> 12,
    "Boston Latin School" -> 14,
    "Bradley Elementary School" -> 16,
    "Brighton High School" -> 17,
    "Burke High School" -> 18,
    "Channing Elementary School" -> 20,
    "Charlestown High School" -> 21,
    "Chittick Elementary School" -> 22,
    "Clap Elementary School" -> 23,
    "Community Academy" -> 24,
    "Condon Elementary School" -> 26,
    "Conley Elementary School" -> 27,
    //"Curley School K-8" -> 28, //has two buildings
    "Dearborn Middle School" -> 30,
    //"DEC High School" -> 33, //????
    "Dever Elementary School" -> 31,
    //Dickerman Elementary School nor Dorchester Academy in check book
    "Dudley St. Neighborhood School" -> 34,
    "East Boston EEC" -> 35,
    "East Boston High School" -> 36,
    "Edison K - 8" -> 37,
    "Edwards Middle School" -> 38,
    //"Eliot K-8" -> 40 //has three buildings
    "Ellis Elementary School" -> 42,
    "Ellison/Parks EES" -> 43,
    //Endicott?
    //Emerson Elementary School?
    "English High School" -> 45,
    "Everett Elementary School" -> 46,
    //Excel?
    //Farragut Elementary School?
    "Fenway-Pilot High School" -> 48,
    //Fifield Elementary School //Find this again
    "Frederick Pilot Middle" -> 49,
    "Gardner Pilot  Academy" -> 50,
    //Gavin Middle School?
    "Greater Egleston High" -> 51,
    "Green Academy" -> 11,
    "Greenwood E Leadership Acad" -> 52,
    "Greenwood Schl K-8" -> 53,
    "Greenwood, S K-8" -> 53,
    "Grew Elementary School" -> 54,
    "Guild Elementary School" -> 55,
    //HPEC Schools?
    "Hale Elementary School" -> 56,
    "Haley Pilot" -> 57,
    //Harbor High?
    //Hamilton, Alexander
    "Harvard-Kent School" -> 59,
    "Haynes EEC" -> 60,
    //Health Careers-Pilot  High Sch
    //"Henderson Elementary School" -> 61, //Two buildings
    "Hennigan Elementary School" -> 63,
    "Hernandez K-8" -> 64,
    "Higginson Elementary School" -> 65,
    "Higginson/Lewis K - 8" -> 66,
    //Holland Elementary School
    "Holmes Elementary School " -> 67,
    //Horace Mann School?
    "Hurley K-8" -> 69,
    "Irving Middle School" -> 70,
    "Jackson/Mann K-8" -> 71,
    "Kennedy JF Elementary" -> 74,
    "Kennedy, JF Elementary" -> 74,
    "Kennedy PJ Elementary" -> 75,
    "Kenny Elementary School" -> 76,
    //Kilmer K-8 has two buildings
    "King K - 8" -> 79,
    "Latin Academy" -> 16,
    "Lee Academy" -> 80, //Fifield?,
    "Lee Elementary School" -> 81,
    "Lyndon K-8" -> 82,
    "Lyon K-8" -> 84,
    "Lyon Pilot High 9-12" -> 83,
    "M Park High-Crafts Academy" -> 85,
    "M Park High-Freshman Academy" -> 85,
    "M Park High-Health Academy" -> 85,
    "Madison Park High School" -> 85,
    "Manning Elementary School" -> 86,
    "Margarita Muniz Academy" -> 99,
    //Marshall Elementary School?
    "Mason Elementary School" -> 87,
    "Mather Elementary School" -> 88,
    "Mattahunt Elementary School" -> 89,
    "Mccormack Middle School" ->  90,
    "McKay Elementary School" -> 91,
    //McKinley Voc School messy
    "Mendell Elementary School" -> 95,
    //Middle School Academy?
    "Mildred Avenue K - 8" -> 96,
    "Mission Hill K-8" -> 97,
    "Mozart Elementary School" -> 98,
    "Murphy Elementary School" -> 100,
    "New Mission High School" -> 101,
    "Newcomers Academy" -> 12,
    "O'Bryant School" -> 102,
    "O'Donnell Elementary School" -> 103,
    "Ohrenberger Elementary School" -> 104,
    "Orchard Gardens K-8 Pilot Schl" -> 105,
    "Otis Elementary School" -> 106,
    //P. A. Shaw Elementry
    "Perkins Elementary School" -> 107,
    "Perry K-8" -> 108,
    "Philbrick Elementary School" -> 109,
    "Quincy Elementary School" -> 110,
    "Quincy Upper School" -> 112, //Has "modulars"
    "Rogers Middle School" -> 113,
    //Roland Hayes Division of Music
    //Roosevelt K-8 has two schools
    "Russell Elementary School" -> 116,
    //Snowden International School has three buildings
    //South Boston HS?
    //Stone, Lucy Elementary School?
    "Sumner Elementary School" -> 122,
    "Taylor Elementary School" -> 123,
    "Tech Boston Academy" -> 124,
    //"The Harbor School" -> "Harbor High" above
    "Timilty Middle School" -> 125,
    "Tobin K-8" -> 126,
    "Trotter Elementary School" -> 127,
    "Tynan Elementary School" -> 128,
    "UP Academy Charter" -> 129,
    "UP Academy Dorchester" -> 130,
    "UP Academy Holland" -> 131,
    "Umana Middle" -> 132,
    //Rest of WREC?
    "WREC Urban Science Academy" -> 133,
    //Warren/Prescott K-8 has two buildings
    "West Roxbury Academy" -> 136,
    "West Zone ELC" -> 137//,
//    "Winship Elementary School" -> 139,
//    "Winthrop Elementary School" -> 140,
//    "Young Achievers K-8" -> 141
  )

  def getCorrectName(program: String) : Int = {
    schoolMap.getOrElse(program, -1)
  }

  def main(args: Array[String]): Unit = {

    // https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha
    System.setProperty("hadoop.home.dir", "E:\\Program Files\\Hadoop")

    val checkBook = classifyCheckBook(checkbook.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it))
    val buildEntries = classifyBPS(bpsinfo.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it))
    val bpsEntries = checkBook.filter(entry => {
      val banned = Array("AcadSup", "BPS", "Superintendent", "Officer")
      val hasBanned = banned.exists(str => {
        entry.program.contains(str)
      })
      entry.deptName == "Boston Public School Dept" && !hasBanned
    }).persist()

    val betterNames = buildEntries.map(entry => {
      entry.copy(school = {
        val n = entry.school.filter(c => {c != '*'}).trim
        val endings = Array("High", "Middle", "Elementary")
        val needsSchool = endings.exists(str => {
          n.endsWith(str)
        })
        if (needsSchool) n + " School"
        else n
      })
    }).collect().sortBy(_.school)

    val mapped = mapCheckBook(bpsEntries, betterNames).persist()

    //mapped.groupBy({case (info, entry) => entry.accountDesc}).keys.collect().foreach(println)
    //mapped.groupByKey.mapValues(_.map(_.accountDesc).toSet).collect().map(_._2).foreach(println)

//    val byMonth = getMonthCosts(mapped).toDF
//    val byYear = getYearCosts(mapped).toDF
//    val avgCosts = getAvgCost(mapped).toDF
//    val simples = getSimpleCosts(mapped).toDF
//    sc.parallelize(betterNames).toDF().coalesce(1).write.csv("schools")

//    byYear.coalesce(1).write.csv("years")
//    byMonth.coalesce(1).write.csv("months")
//    avgCosts.coalesce(1).write.json("avg")
//    simples.coalesce(1).write.csv("simple")
//    getDescCostsAvg(mapped).toDF().coalesce(1).write.csv("descs")
//    normalizeDescCosts(getDescCostsAvg(mapped)).toDF.coalesce(1).write.csv("normalized")


//    val bpsGrouped = bpsEntries.map(entry => {
//      (entry.program, (1, entry.amount))
//    }).reduceByKey((a, b) => {
//      (a._1 + b._1, a._2 + b._2)
//    }).mapValues(pair => (pair._1, Math.round(pair._2))).sortByKey().collect//.slice(155, 185)

    //val firsts = betterNames//.slice(125, 150)

//    val groupedInfo = mapCheckBook(bpsEntries, betterNames).groupByKey()//.collect().sortBy(_._1.name)
//
//    val moneys = groupedInfo.map(pair => (pair._1.school, (pair._2.foldRight(0.0)(_.amount + _).round, pair._1.students))).collect.sortBy(_._1)
//
//    moneys.foreach(entry => {
//      if (entry._2._2 > 0)
//      println(entry._1 + " " + (entry._2._1 / entry._2._2))
//    })

//    bpsGrouped.foreach(tuple => {
//      println(tuple._1 + " " + tuple._2)
//    })
//
//    println("*********")
//    firsts.foreach(info => {
//      println(info.name)
//    })
//
//    println("*********")
//    println(groupedInfo.length + " schools joined")
//    groupedInfo.foreach(key => {
//      println(key._1.name + "          " + key._2.head.program)
//    })

    sc.stop() //DO NOT FORGET!!!!!!!!!
  }
}