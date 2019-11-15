import sys

def addBorder(linesToWrite):
    linesToWrite.append("\n\n================================================================\n\n")


def Average(lst):
    return sum(lst) / len(lst)


def parseEpisode(episode: str, linesToWrite: list, allLines, numberOfThreads):
    addBorder(linesToWrite)
    for i in range(numberOfThreads):
        lineSecondsPrefix: str = "{} episode, Thread name T{}, elapsed time:".format(episode, i)
        lineAbortPrefix: str = "{} episode, Thread name T{}, abort counts:".format(episode, i)
        linesSeconds = list(filter(lambda line: line.startswith(lineSecondsPrefix), allLines))
        linesAbort = list(filter(lambda line: line.startswith(lineAbortPrefix), allLines))
        avgSeconds = Average(list(map(lambda line: (int)(line.split(":")[1].split("[")[0].strip()), linesSeconds)))
        avgAbort = Average(list(map(lambda line: (int)(line.split(":")[1].strip()), linesAbort)))
        linesToWrite.append("{} {} [ms]\n".format(lineSecondsPrefix, avgSeconds))
        linesToWrite.append("{} {}\n".format(lineAbortPrefix, avgAbort))


if __name__ == '__main__':
    numberOfThreads = (int)(sys.argv[1])
    pow = (int)(sys.argv[2])
    files_to_parse = (int)(sys.argv[3])

    FILES = []
    for fileIndex in range(files_to_parse):
        FILES.append("OurBenchmark_{}T_{}E_{}.txt".format(numberOfThreads, pow,fileIndex))



    out = "OurBenchmark_{}T_{}E.txt".format(numberOfThreads, pow)


    linesToWrite = []

    openFiles = []
    for filenames in FILES:
        openFiles.append(open(filenames, "r"))

    allLines = []

    for openFile in openFiles:
        allLines += openFile.readlines()
        openFile.close()

    linesToWrite.append(allLines[0])
    parseEpisode("Enqueue", linesToWrite, allLines, numberOfThreads)
    parseEpisode("Decrease Priority", linesToWrite, allLines, numberOfThreads)
    parseEpisode("Top", linesToWrite, allLines, numberOfThreads)
    parseEpisode("Dequeue", linesToWrite, allLines, numberOfThreads)

    outputfile = open(out, "w")
    outputfile.writelines(linesToWrite)
