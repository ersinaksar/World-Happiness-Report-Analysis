from Project_ErsinAksarWorldHappinessReport import *

if __name__ == "__main__":
    appName = "WorldHappinessReport"
    #create object from Project_ErsinAksarWorldHappinessReport class
    wh = WorldHappinessReport(appName)
    #take spark from Project_ErsinAksarWorldHappinessReport as spark
    spark= wh.spark
    wh.readAndPreprocessingDatasets()

    # Prepare spark context
    sc = spark.sparkContext

    wh.selectReportNo()
    reportNo = eval(input("Select report No: "))
    while(reportNo != 0):
        wh.showHappinessReports(reportNo)
        reportNo = eval(input("Select report No: "))







    # Average happiness scores of the top 10 countries by years
    """print("--------Average happiness scores of the top 10 countries by years--------\n")
    rank10 = rank.filter((rank["Happiness Rank"] == 1) | (rank["Happiness Rank"] == 2) | (rank["Happiness Rank"] == 3)
                         | (rank["Happiness Rank"] == 4) | (rank["Happiness Rank"] == 5) | (rank["Happiness Rank"] == 6)
                         | (rank["Happiness Rank"] == 7) | (rank["Happiness Rank"] == 8) | (rank["Happiness Rank"] == 9)
                         | (rank["Happiness Rank"] == 10))

    # MEAN DEĞERİ İÇİN NOKTADAN SONRA DİGİT LİMİT YAPILMASI LAZIMM
    rank10.groupBy("Country").pivot("Year").mean("Happiness Score").show()"""
