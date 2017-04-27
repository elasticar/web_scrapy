
def extractCoreProp(propName, dataList):
    retPropName = "Nepateikta"
    if propName in dataList:
        propIndex = dataList.index(propName) + 1
        retPropName = dataList[propIndex]
    return retPropName