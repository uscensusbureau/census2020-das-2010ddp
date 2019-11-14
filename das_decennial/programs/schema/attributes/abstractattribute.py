from abc import abstractmethod, ABCMeta

class AbstractAttribute(metaclass=ABCMeta):
    """ This is a UI towards creating querybase objects. Recodes must start with 'recode' and should be static methods """

    @staticmethod
    @abstractmethod
    def getName():
        """ Return the name of the attribute """
        pass

    @staticmethod
    @abstractmethod
    def getLevels():
        """ return a dict of String : List """
        pass

    @classmethod
    def getRecodes(cls):
        """ calls every function (staticmethod) that starts with 'recode' and returns list of results """
        recodelist = [x for x in dir(cls) if x.startswith("recode")]
        recoderesults =  [getattr(cls, x)() for x in recodelist]
        return [(name, {cls.getName(): groups}) for (name, groups) in recoderesults]

    @classmethod
    def getSize(cls):
        """ returns the number of levels of this attribute """
        return len(cls.getLevels())