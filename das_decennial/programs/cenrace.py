from itertools import product
import numpy as np


#################
# Cenrace class
#################
class Cenrace():
    """
    Creates and houses dictionaries for accessing the CENRACE recodes 
    Notes:
        number of categories is 2^(number of labels) - 1 because, by definition,
        no person is allowed to respond as having no race
    """
    def __init__(self, race_labels=["white", "black", "aian", "asian", "nhopi", "sor"]):
        self.race_labels = race_labels
        self.num_categories = (2 ** len(self.race_labels)) - 1
        self.race_dict = self.buildRaceDict(self.race_labels, reverse_keyval=False)
        self.rev_race_dict = self.buildRaceDict(self.race_labels, reverse_keyval=True)
        
        self.race_categories = self.getRaceCategories(racesOnly=False)
        
    
    def __repr__(self):
        items = [ "Cenrace",
                  "Race Labels:     {}".format(self.race_labels),
                  "Race Categories: {}".format("\n                 ".join([str(rc) for rc in self.race_categories])) ]
        
        return "\n".join(items)
    
    def getCenrace(self, key):
        assert isinstance(key, str) or isinstance(key, int)
        
        if isinstance(key, str):
            return self.stringToCenrace(key, self.race_labels)
        else: # type is int
            return self.stringToCenrace(self.rev_race_dict[key], self.race_labels)
    
    def translateKey(self, key):
        assert isinstance(key, str) or isinstance(key, int)
        
        if isinstance(key, str):
            return self.race_dict[key]
        else: # type is int
            return self.rev_race_dict[key]
    
    def stringToCenrace(self, string, labels):
        return "-".join([labels[i] for i,x in enumerate(string) if int(x) == 1])
    
    def buildRaceDict(self, race_labels, reverse_keyval=False):
        """
        Modified from William's recode function(s)
        """
        race_map = ["".join(x) for x in product("10", repeat=len(race_labels))]
        race_map.sort(key=lambda s: s.count("1"))
        race_dict = {}
        
        for i,k in enumerate(race_map[1:]):
            ind = i + 1
            if reverse_keyval:
                race_dict[ind] = k
            else:
                race_dict[k] = ind
        
        return race_dict
    
    def getCenraceDict(self, reverse_map=False):
        if reverse_map:
            return self.race_dict
        else:
            return self.rev_race_dict
    
    def getRaceCategories(self, racesOnly=True):
        """
        Notes:
        """
        if racesOnly:
            return [self.getCenrace(i+1) for i in range(self.num_categories)]
        else:
            return [(i+1, self.getCenrace(i+1)) for i in range(self.num_categories)]
    
    def getRace(self, keys):
        races = []
        for key in keys:
            race = [x for x in self.race_categories if x[0] == key or x[1] == key]
            if len(race) > 0:
                races.append(race)
        
        return races
    
    def toDict(self, reversed=False):
        if reversed:
            return dict([(x[1], x[0]) for x in self.race_categories])
        else:
            return dict(self.race_categories)
    
    def toJSON(self):
        jsondict = { "Race Labels": self.race_labels,
                     "Race Categories": self.race_categories }
        return json.dumps(jsondict)


def getCenraceLevels(race_labels=["white", "black", "aian", "asian", "nhopi", "sor"]):
    return Cenrace(race_labels=race_labels).getRaceCategories()

def getCenraceNumberGroups(race_labels=["white", "black", "aian", "asian", "nhopi", "sor"]):
    categories = Cenrace(race_labels=race_labels).getRaceCategories()
    lengths = [len(x.split("-")) for x in categories]
    lengthset = list(set(lengths))
    
    groups = []
    for length in lengthset:
        label = "{} Race".format(length) if length == 1 else "{} Races".format(length)
        indices = [index for index,leng in enumerate(lengths) if leng == length]
        cats = np.array(categories)[indices].tolist()
        groups.append((label, indices, cats))
    
    group_names, group_indices, group_categories = [list(x) for x in zip(*groups)]
    
    group = CenraceGroup(group_names, group_indices, group_categories)
    
    return group


def getCenraceAloneOrInCombinationGroups(race_labels=["white", "black", "aian", "asian", "nhopi", "sor"]):
    levels = getCenraceLevels(race_labels=race_labels)
    labels = []
    indices = []
    categories = []
    for race in race_labels:
        labels.append(f"{race} alone or in combination with one or more other races")
        items = [(i,x) for i,x in enumerate(levels) if race in x.split("-")]
        ind, cat = [list(x) for x in zip(*items)]
        indices.append(ind)
        categories.append(cat)
    
    group = CenraceGroup(labels, indices, categories)
    
    return group



class CenraceGroup():
    def __init__(self, group_names, group_indices, group_categories):
        self.group_names = group_names
        self.group_indices = group_indices
        self.group_categories = group_categories
    
    def getGroupLabels(self):
        return self.group_names
    
    def getIndexGroups(self):
        return self.group_indices
    
    def getCategories(self):
        return self.group_categories