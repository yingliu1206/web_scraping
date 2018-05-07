import csv
import datetime
import multiprocessing
import os
import time
import traceback
from sys import platform

'Driver'
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains

'Driver Exceptions'
from selenium.common.exceptions import *

'Parser'
from bs4 import BeautifulSoup
from bs4.element import Comment

"Display for headless mode"
from pyvirtualdisplay import Display

"Only use this if running on a non linux machine"
driverPath = 'Driver/chromedriver'

inline_tags = ["b", "big", "i", "small", "tt", "abbr", "acronym", "cite", "dfn",
               "em", "kbd", "strong", "samp", "var", "bdo", "map", "object", "q",
               "span", "sub", "sup"]


def readCSV(filename) -> list:
    schools = []
    with open(filename, newline='', encoding="Latin-1") as csvFile:
        reader = csv.reader(csvFile, delimiter=',')
        for row in reader:
            try:
                if reader.line_num != 1:
                    s = School(row[0], row[1], row[2], row[4])
                    schools.append(s)
                    print(s)
            except ValueError:
                print("ERROR: School " + str(row[1]) + " was not scraped as it did not have a URL")
    return schools

#Only used for debugging multithreading
def specialPrint(message: str, threadNumber: int) -> None:
    print(message + "(from thread " + str(threadNumber) + ")")


class School(object):
    """Class that holds schools. Each school is comprised of an ID number, Name, Geographical Address and a url that goes to the schools hompage. The matcher is used to
    filer out links that go to places outside the schools main domain, like facebook or instagram. The links attribute is an array used to store all of the links on the homepage using
    the Links class"""

    def __init__(self, id, name, address, mainURL):
        if mainURL == str(0):
            raise ValueError("ERROR: URL cannot be 0")
        self.id = id
        self.name = name
        self.address = address
        self.mainURL = mainURL
        self.links = []
        if self.mainURL.split("://")[1].startswith("www"):
            self.matcher = self.mainURL.split(".")[1]
        else:
            self.matcher = self.mainURL.split("://")[1].split(".")[0]
        self.filePath = "results/" + self.name
        self.totalNumberofLinks = 0
        self.htmlLinks = 0
        self.htmlLinksClicked = 0
        self.scriptLinks = 0
        self.scriptLinksClicked = 0
        self.linksClicked = 0

    def gatherLinks(self, driver, threadNumber) -> None:
        driver.get(self.mainURL)
        oldElems = driver.find_elements_by_xpath("//a[@href]")
        hrefAttributes = []
        count = 0
        for x in oldElems:
            try:
                if count == 0:
                    hrefAttributes.append(oldElems[count].get_attribute("href"))
                else:
                    hrefAttributes.append(newElems[count].get_attribute("href"))
            except IndexError:
                break
            newElems = driver.find_elements_by_xpath("//a[@href]")
            count += 1
        for i in range(len(hrefAttributes)):
            try:
                link = Link(hrefAttributes[i], self.mainURL, self.matcher, i)
                self.links.append(link)
                specialPrint(str(link), threadNumber)
            except LinkException:
                specialPrint(str(hrefAttributes[i]) + (
                    "href") + " was not added as it did not match the main url or was not longer than main url",
                             threadNumber)
        self.totalNumberofLinks = len(self.links)

    def clickLinks(self, driver, threadNumber):

        if not checkPathExists(self.filePath):
            os.makedirs(self.filePath)
        counter = 1
        for link in self.links:
            try:
                if link.type == "html":
                    self.htmlLinks += 1
                elif link.type == "JavaScript":
                    self.scriptLinks += 1
                specialPrint("Clicking Link " + str(counter) + " out of " + str(self.totalNumberofLinks), threadNumber)
                link.click(driver)
                self.linksClicked += 1
                if link.type == "html":
                    self.htmlLinksClicked += 1
                elif link.type == "JavaScript":
                    self.scriptLinksClicked += 1
            except LinkException:
                specialPrint("Could not click link:" + str(link), threadNumber)
            counter += 1
        scriptCount = 0
        specialPrint("Done Clickling links", threadNumber)
        specialPrint("Writing links to file", threadNumber)
        for link in self.links:
            if link.type == "html":
                link.writeFile(self.filePath, 0)
            elif link.type == "JavaScript" and link.text != "":
                link.writeFile(self.filePath, scriptCount)
                scriptCount += 1

    def __str__(self) -> str:
        s = ""
        s += "Name:" + self.name + " "
        s += "mainURL:" + self.mainURL + " "
        s += "Matcher:" + self.matcher + " "
        s += "links:" + str(self.links) + " "
        s += "ID:" + self.id + " "
        s += "Address:" + self.address + " "
        return s


class LinkException(Exception):
    "Only called by link class. Add to switch statement as necessary"

    def __init__(self, switch=-1):
        if switch == 0:
            self.value = "ERROR: Link type was not html or JavaScript"
        elif switch == 1:
            self.value = "ERROR: Link was Unclickable"
        elif switch == 2:
            self.value = "ERROR: Link is JavaScript based but an index value was not set"
        elif switch == -1:
            self.value = "No value was specified in LinkException Switch. Make sure you are properly calling this exception"

    def __str__(self) -> str:
        return str(self.value)


class Link(object):
    """Class that stores all of the information regarding a link. Each link has a type (either html of JavaScript), the href attribute (what the link redirects
    to), a fallback url, and an index value (used for JavaScript Links)"""

    def __init__(self, hrefAttribute, callingURL, matcher, index):
        self.type = ""
        self.hrefAttribute = ""
        self.fallbackURL = callingURL
        self.index = None
        self.matcher = matcher
        self.index = 0
        self.text = ""
        if hrefAttribute.startswith("http"):
            if (hrefAttribute.split("://")[1].startswith("www") and hrefAttribute.split(".")[1] == matcher and len(
                    hrefAttribute) > len(callingURL)) or (hrefAttribute.split("://")[1].split(".")[0] == matcher
                                                          and len(hrefAttribute) > len(callingURL)):
                self.type = "html"
                self.hrefAttribute = hrefAttribute
            else:
                raise LinkException(0)
        elif hrefAttribute.startswith("javascript"):
            self.type = "JavaScript"
            self.hrefAttribute = hrefAttribute
            self.index = index
        else:
            raise LinkException(0)
        self.gatherName(delimiter="-")

    def tag_visible(self, element) -> bool:
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True

    def gatherText(self, driver) -> None:
        page_source_replaced = driver.page_source
        # Remove inline tags
        for it in inline_tags:
            page_source_replaced = page_source_replaced.replace("<" + it + ">", "")
            page_source_replaced = page_source_replaced.replace("</" + it + ">", "")

        # Create random string for tag delimiter
        random_string = "".join(map(chr, os.urandom(75)))
        soup = BeautifulSoup(page_source_replaced, 'lxml')
        # remove non-visible tags
        [s.extract() for s in soup(['style', 'script', 'head', 'title', 'meta', '[document]'])]
        visible_text = soup.getText(random_string).replace("\n", "")
        visible_text = visible_text.split(random_string)
        self.text = "\n".join(list(filter(lambda vt: vt.split() != [], visible_text)))

    def click(self, driver) -> bool:
        if self.type == "html":
            driver.get(self.hrefAttribute)
            self.gatherText(driver)
            return True
        elif self.type == "JavaScript":
            if self.index is None:
                raise LinkException(2)
            driver.get(self.fallbackURL)
            try:
                driver.find_elements_by_xpath("//a[@href]")[self.index].click()
                self.gatherText(driver)
            except (WebDriverException, ElementNotVisibleException, ElementNotInteractableException,
                    ElementNotSelectableException, IndexError):
                link = driver.find_elements_by_xpath("//a[@href]")[self.index]
                move = ActionChains(driver).move_to_element(link)
                move.perform()
                try:
                    link.click()
                    self.gatherText(driver)
                except (WebDriverException, ElementNotVisibleException, ElementNotInteractableException,
                        ElementNotSelectableException, IndexError):
                    raise LinkException(1)
        else:
            raise LinkException(0)

    def gatherName(self, delimiter=" ") -> None:
        if delimiter == "/":
            raise ValueError("ERROR: Delimiter cannot be a slash")
        if self.type == "html":
            unfilteredName = self.hrefAttribute[self.hrefAttribute.index(self.matcher):len(self.hrefAttribute)]
            unfilteredName = unfilteredName.split("/")
            self.name = ""
            if len(unfilteredName) != 1:
                for i in range(len(unfilteredName)):
                    self.name += unfilteredName[i] + delimiter
            else:
                self.name = unfilteredName[0]
        elif self.type == "JavaScript":
            self.name = ""

    def writeFile(self, filepath, counter):
        fileName = self.name
        if self.type == "html":
            file = open(str(filepath) + "/" + fileName + ".txt", "w")
        elif self.type == "JavaScript":
            file = open(str(filepath) + "/" + "JavaScript Link " + str(counter) + ".txt", "w")
        else:
            raise LinkException(0)
        file.write(str(self.text.encode('utf-8'), encoding='utf-8'))
        file.close()

    def __str__(self) -> str:
        s = ""
        s += "Link Type:" + self.type + " "
        s += "hrefAttribute:" + self.hrefAttribute + " "
        s += "name:" + self.name + " "
        s += "FallbackURL(Only used for JS):" + self.fallbackURL + " "
        s += "Index (Only used for JS):" + str(self.index) + " "
        return s


# Helper Functions
def tag_visible(element) -> bool:
    return element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]'] or isinstance(element,
                                                                                                           Comment)


def checkPathExists(path):
    return os.path.exists(path)


def prepDriver() -> list:
    driver = None
    display = None
    if platform.startswith("linux"):
        display = Display(visible=0, size=(1920, 1080))
        display.start()
        chromeOptions = webdriver.ChromeOptions()
        chromeOptions.add_argument('headless')
        chromeOptions.add_argument('window-size=1920x1080')
        chromeOptions.add_argument('--no-sandbox')
        driver = webdriver.Chrome('/usr/local/bin/chromedriver', chrome_options=chromeOptions)
    elif platform.startswith("darwin") or platform.startswith("win32"):
        driver = webdriver.Chrome(executable_path="Driver/chromedriver")
    if not driver:
        raise ValueError("ERROR: Driver was not initialized before use")
    if not display and platform.startswith("linux"):
        raise ValueError("ERROR: Display was not initialized before use")
    return [driver, display]


# Worker function for each core


def assign_indexes(numberOfThreads: int, numberOfSchools: int):
    # Divides up the work of scraping schools between available threads
    # Last core will have more work to do if numberOfSchools % numberOfCores != 0
    result = []
    scaler = int(numberOfSchools / numberOfThreads)

    for i in range(1, numberOfThreads + 1):
        result.append([scaler * (i - 1), (scaler * i) - 1])

    if numberOfSchools % numberOfThreads != 0:
        result[len(result) - 1][1] = numberOfSchools - 1
    return result

def worker(processNumber, schools, driver, display=None, fileName=None):
    index = 0
    try:
        for school in schools:
            school.gatherLinks(driver, processNumber)
            schoolStartTime = time.time()
            school.clickLinks(driver, processNumber)
            endTime = time.time()
            schoolTimeElapsed = endTime - schoolStartTime
            specialPrint("Elapsed Time :%s (seconds) %s (minutes)" % (
                str(schoolTimeElapsed), str(schoolTimeElapsed / 60)), processNumber)
            index += 1
            totalNumberOfLinks[processNumber] += school.totalNumberofLinks
            numberofLinksClicked[processNumber] += school.linksClicked
            htmlLinks[processNumber] += school.htmlLinks
            htmlLinksClicked[processNumber] += school.htmlLinksClicked
            scriptLinks[processNumber] += school.scriptLinks
            scriptLinks[processNumber] += school.scriptLinksClicked
            """
            try:
                tempDiagnosticsFile.write(
                    "School " + str(school.name) + " had " + str(school.totalNumberofLinks) + " links and " + str(
                        school.linksClicked) + " were clicked(" + str(
                        (school.linksClicked / school.totalNumberofLinks) * 100) + "%)\n")
            except ZeroDivisionError:
                tempDiagnosticsFile.write(
                    "School " + str(school.name) + " had 0 links. Check the matcher for this school ")
            try:
                tempDiagnosticsFile.write(
                    "There were " + str(school.htmlLinks) + " html links and " + str(
                        school.htmlLinksClicked) + " were clicked(" + str(
                        round((school.htmlLinksClicked / school.htmlLinks) * 100, 3)) + "%)\n"
                )
            except ZeroDivisionError:
                tempDiagnosticsFile.write("This school had 0 html links \n")

            try:
                tempDiagnosticsFile.write(
                    "There were " + str(school.scriptLinks) + " JavaScript links and " + str(
                        school.scriptLinksClicked) + " were clicked(" + str(round(
                        (school.scriptLinksClicked / school.scriptLinks) * 100, 3)) + "%)\n"
                )
            except ZeroDivisionError:
                tempDiagnosticsFile.write("This school had 0 JavaScript Links \n")

            tempDiagnosticsFile.write("It took " + str(round(schoolTimeElapsed / 60, 3)) +
                                      " minutes to click on the links for this school\n")
            tempDiagnosticsFile.close()
            """
    except Exception as e:
        print("Thread " + str(processNumber) + " ran into an exception on school " + str(schools[index]))
        e = traceback.format_exc()
        errors.write(e)
        print(e)
        worker(processNumber, schools[index + 1:], driver, display, fileName)

# This line may cause a warning in certain IDEs saying something like "__main__ does not exist" Ignore this warning
if __name__ == '__main__':
    if not checkPathExists("results"):
        os.mkdir("results")
    if not checkPathExists("diagnostics"):
        os.mkdir("diagnostics")
    schools = readCSV("data/micro-sample13_coded.csv")
    numberofLinksClicked = 0
    totalNumberOfLinks = 0
    htmlLinks = 0
    htmlLinksClicked = 0
    scriptLinks = 0
    scriptLinksClicked = 0
    threads = multiprocessing.cpu_count()
    "Time doesn't really account for timezones now, many be an issue later"
    now = datetime.datetime.now()
    formattedTime = now.strftime("%Y-%m-%d %H:%M:%S")
    errors = open("ERRORS.txt", "w")
    diagnosticsFile = open("diagnostics/" + str(formattedTime) + ".txt", "w")
    diagnosticsFile.write("Program was run at " + formattedTime + "\n")
    startTime = time.time()
    totalNumberOfLinks = [0 for x in range(threads)]
    numberofLinksClicked = [0 for x in range(threads)]
    htmlLinks = [0 for x in range(threads)]
    htmlLinksClicked = [0 for x in range(threads)]
    scriptLinks = [0 for x in range(threads)]
    scriptLinks = [0 for x in range(threads)]

    work = assign_indexes(threads, len(schools))
    jobs = []
    for i in range(len(work)):
        driver = prepDriver()
        p = multiprocessing.Process(target=worker,
                                    args=(i, schools[work[i][0]:work[i][1]], driver[0], driver[1],
                                          "diagnostics/thread" + str(i) + "TempFile.txt"))
        jobs.append(p)
        p.start()
        print("Started process " + str(i) + " to click links for schools " + str(work[i][0]) + " to " + str(work[i][1]))
    for p in jobs:
        p.join()

    timeElapsed = time.time() - startTime
    diagnosticsFile.write("Total number of links:" + str(sum(totalNumberOfLinks)) + "\n")
    diagnosticsFile.write("Number of Links Clicked:" + str(sum(numberofLinksClicked)) + "\n")
    # TODO: Fix diagnostic files for multiple threads (make each thread have a file and merege at the end
    """try:
        diagnosticsFile.write(
            "% of links clicked:" + str(round((sum(numberofLinksClicked) / sum(totalNumberOfLinks)) * 100, 3)) + "\n")
    except ZeroDivisionError:
        diagnosticsFile.write("There were 0 Total  Links\n")
    diagnosticsFile.write("Number of HTML Links" + str(htmlLinks) + "\n")
    try:
        diagnosticsFile.write("% of HTML Links Clicked" + str(round((htmlLinks / htmlLinksClicked) * 100, 3)) + "\n")
    except ZeroDivisionError:
        diagnosticsFile.write("There were 0 HTML links")
    diagnosticsFile.write("Number of JavaScript Links" + str(scriptLinks) + "\n")
    try:
        diagnosticsFile.write(
            "% of JavaScript Links Clicked" + str(round((scriptLinks / scriptLinksClicked) * 100, 3)) + "\n")
    except ZeroDivisionError:
        diagnosticsFile.write("There were 0 JavaScript Links")
    diagnosticsFile.write("It took " + str(round(timeElapsed, 3)) + " minutes to click all links")"""
    diagnosticsFile.close()
