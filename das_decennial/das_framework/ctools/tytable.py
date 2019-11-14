#!/usr/bin/env python3
"""
tytable.py:

DO NOT START NEW PROJECTS WITH THIS! USE TYDOC INSTEAD

Module for typesetting tables in ASCII, LaTeX, and HTML.  Perhaps even CSV!
Also creates LaTeX variables.

Simson Garfinkel, 2010-

This is really bad python. Let me clean it up before you copy it.
ttable is the main typesetting class. It builds an abstract representation of a table and then typesets with output in Text, HTML or LateX. 
It can do fancy things like add commas to numbers and total columns.
All of the formatting specifications need to be redone so that they are more flexbile
"""
from typing import List, Dict, Any, Iterable

import os.path
import sys
import sqlite3
import xml.etree.ElementTree
import xml.etree.ElementTree as ET
import xml.dom.minidom

# not sure why this was put in, but it breaks calling tytable from this directory.
# if __name__ == "__main__" or __package__=="":
#    __package__ = "ctools"

sys.path.append(os.path.dirname(__file__))

import latex_tools

__version__ = "0.2.1"


#
# Some basic functions for working with text and numbers
#

def isnumber(v):
    """Return true if we can treat v as a number"""
    # TODO: This returns True for a str, not sure if it is intended behavior
    #  maybe
    #  return v - 0 == v
    #  is better, or
    #  return abs(v - v) < 1e-7
    try:
        return v == 0 or v != 0
    except TypeError:
        return False


def safeint(v):
    """Return v as an integer if it is a number, otherwise return it as is"""
    try:
        return int(v)
    except (ValueError, TypeError):
        return v


def safefloat(v):
    """Return v as a float if it is a number, otherwise return it as is"""
    try:
        return float(v)
    except (ValueError, TypeError):
        return v


def safenum(v):
    """Return v as an int if possible, then as a float, otherwise return it as is"""
    try:
        return int(v)
    except (ValueError, TypeError):
        pass
    try:
        return float(v)
    except (ValueError, TypeError):
        pass
    return v


def scalenum(v, minscale=0):
    """Like safenum, but automatically add K, M, G, or T as appropriate"""
    v = safenum(v)
    if type(v) == int:
        for (div, suffix) in [[1_000_000_000_000, 'T'], [1_000_000_000, 'G'], [1_000_000, 'M'], [1_000, 'K']]:
            if (v > div) and (v > minscale):
                return str(v // div) + suffix
    return v


def latex_var(name, value, desc=None, xspace=True):
    """Create a variable NAME with a given VALUE.
    Primarily for output to LaTeX.
    Returns a string."""
    xspace_str = r"\xspace" if xspace else ""
    return "".join(['\\newcommand{\\', str(name), '}{', str(value), xspace_str, '}'] + ([' % ', desc] if desc else []) + ['\n'])


def text_var(name, value, desc=None):
    """Create a variable NAME with a given VALUE.
    Primarily for output to LaTeX.
    Returns a string."""
    return "".join(['Note: ', str(name), ' is ', str(value)] + ([' (', desc, ')'] if desc else []))


def icomma(i):
    """ Return an integer formatted with commas """
    if i < 0:
        return "-" + icomma(-i)
    if i < 1000:
        return "%d" % i
    return icomma(i / 1000) + ",%03d" % (i % 1000)


################################################################
### Legacy system follows
################################################################


# The row class holds the row and any annotations
class Row:
    __slots__ = ['data', 'annotations']

    def __init__(self, data, annotations=None):
        if annotations:
            if len(data) != len(annotations):
                raise ValueError("data and annotations must have same length")
        self.data = data
        self.annotations = annotations

    def __len__(self):
        return len(self.data)

    def __getitem__(self, n):
        return self.data[n]

    def ncols(self):
        return len(self.data)


# Heads are like rows, but they are headers
class Head(Row):
    def __init__(self, data, annotations=None):
        super().__init__(data=data, annotations=annotations)


# A special class that makes a horizontal rule
class HorizontalRule(Row):
    def __init__(self):
        super().__init__(data=[])


# Raw is just raw data passed through
class Raw(Row):
    def __init__(self, rawdata):
        self.data = rawdata

    def ncols(self):
        raise RuntimeError("Raw does not implement ncols")


def line_end(mode):
    if mode == ttable.TEXT:
        return "\n"
    elif mode == ttable.LATEX:
        return r"~\\" + "\n"
    elif mode == ttable.HTML:
        return "<br>"
    else:
        raise RuntimeError("Unknown mode: {}".format(mode))


class ttable:
    """ Python class that prints formatted tables. It can also output LaTeX.
    Typesetting:
       Each entry is formatted and then typeset.
       Formatting is determined by the column formatting that is provided by the caller.
       Typesetting is determined by the typesetting engine (text, html, LaTeX, etc).
       Numbers are always right-justified, text is always left-justified, and headings
       are center-justified.

       ## Data building functions:
       ttable() - Constructor. 
       .set_title(title) 
       .compute_and_add_col_totals() - adds columns for specified columns / run automatically
       .compute_col_totals(col_totals) - adds columns for specified columns
       .add_head([row]) to one or more heading rows. 
       .add_data([row]) to append data rows. 
       .add_data(ttable.HR) - add a horizontal line

       ## Formatting functions:
       set_col_alignment(col,align) - where col=0..maxcols and align=ttable.RIGHT or ttable.LEFT or ttable.CENTER
                                (center is not implemented yet)
       set_col_alignments(str)      - sets with a LaTeX-stye format string
       set_col_totals([1,2,3,4]) - compute totals of columns 1,2,3 and 4

       ## Outputting
       typeset(mode=[TEXT,HTML,LATEX]) to typeset. returns table
       save_table(fname,mode=)
       add_variable(name,value)  -- add variables to output (for LaTeX mostly)
       set_latex_colspec(str)    -- sets the LaTeX column specification, rather than have it auto calculated
    """

    # Attributes type annotations (also suppresses "Instant attribute defined outside __init__" warnings)
    mode: str

    data: List[Row]  # the raw data; a list of lists
    omit_row: List[str]  # descriptions of rows that should be omitted

    cols: int  # Number of columns
    col_widths: List[int]  # a list of how wide each of the formatted columns are
    col_margin: int
    col_fmt_default: str  # default format gives numbers
    col_fmt: Dict[int, str]  # format for each column
    col_totals: Iterable
    col_alignment: Dict[int, str]
    col_headings: List[Head]  # the col_headings; a list of lists

    title: str
    num_units: list
    footer: str
    header: str  #
    heading_hr_count: int  # number of <hr> to put between heading and table body
    variables = Dict[str, Any]  # additional variables that may be added
    label: str
    caption: str
    footnote: str
    autoescape: bool
    fontsize: int

    latex_colspec: Any
    col_formatted_widths: List[int ]

    OPTION_LONGTABLE = 'longtable'
    OPTION_TABULARX = 'tabularx'
    OPTION_TABLE = 'table'
    OPTION_CENTER = 'center'
    OPTION_NO_ESCAPE = 'noescape'
    HR = HorizontalRule()
    SUPPRESS_ZERO = "suppress_zero"
    TEXT_MODE = TEXT = 'text'
    LATEX_MODE = LATEX = 'latex'
    HTML_MODE = HTML = 'html'
    MARKDOWN_MODE = MARKDOWN = 'markdown'
    RIGHT = "RIGHT"
    LEFT = "LEFT"
    CENTER = "CENTER"
    NL = {TEXT: '\n', LATEX: "\\\\ \n", HTML: ''}  # new line
    VALID_MODES = {TEXT, LATEX, HTML}
    VALID_OPTIONS = {OPTION_LONGTABLE, OPTION_TABULARX, SUPPRESS_ZERO, OPTION_TABLE}
    DEFAULT_ALIGNMENT_NUMBER = RIGHT
    DEFAULT_ALIGNMENT_STRING = LEFT
    HTML_ALIGNMENT = {RIGHT: "style='text-align:right;'", LEFT: "style='text-align:left;'", CENTER: "style='text-align:center;'"}

    def __init__(self, mode=None):
        self.set_mode(mode)
        self.options = set()
        self.clear()

    def set_mode(self, mode):
        assert (mode in self.VALID_MODES) or (mode is None)
        self.mode = mode

    def clear(self):
        """Clear the data and the formatting; keeps mode and options"""
        self.col_headings = []  # the col_headings; a list of lists
        self.data = []  # the raw data; a list of lists
        self.omit_row = []  # descriptions of rows that should be omitted
        self.col_widths = []  # a list of how wide each of the formatted columns are
        self.col_margin = 1
        self.col_fmt_default = "{:}"  # default format gives numbers
        self.col_fmt = {}  # format for each column
        self.title = ""
        self.num_units = []
        self.footer = ""
        self.header = None  #
        self.heading_hr_count = 1  # number of <hr> to put between heading and table body
        self.col_alignment = {}
        self.variables = {}  # additional variables that may be added
        self.label = None
        self.caption = None
        self.footnote = None
        self.autoescape = True  # default
        self.fontsize = None

    def set_fontsize(self, ft):
        self.fontsize = ft

    def add_option(self, o):
        self.options.add(o)

    def set_option(self, o):
        self.options.add(o)

    def set_data(self, d):
        self.data = d

    def set_title(self, t):
        self.title = t

    def set_label(self, l):
        self.label = l

    def set_footer(self, footer):
        self.footer = footer

    def set_caption(self, c):
        self.caption = c

    def set_col_alignment(self, col, align):
        self.col_alignment[col] = align

    def set_col_alignmnets(self, fmt):
        col = 0
        for ch in fmt:
            if ch == 'r':
                self.set_col_alignment(col, self.RIGHT)
                col += 1
                continue
            elif ch == 'l':
                self.set_col_alignment(col, self.LEFT)
                col += 1
                continue
            else:
                raise RuntimeError("Invalid format string '{}' in '{}'".format(fmt, ch))

    def set_col_totals(self, totals):
        self.col_totals = totals

    def set_col_fmt(self, col, fmt):
        """Set the formatting for column COL. Format is specified with a Python format string.
        You can create a prefix and suffix by putting them on either side of the formatter.
        e.g. prefix{:,}suffix.
        """
        self.col_fmt[col] = fmt

    def set_latex_colspec(self, latex_colspec):
        self.latex_colspec = latex_colspec

    def add_head(self, values, annotations=None):
        """ Append a row of VALUES to the table header. The VALUES should be a list of columsn."""
        assert isinstance(values, list) or isinstance(values, tuple)
        self.col_headings.append(Head(values, annotations=annotations))

    def add_subhead(self, values, annotations=None):
        self.data.append(Head(values, annotations=annotations))

    def add_data(self, values, annotations=None):
        """ Append a ROW to the table body. The ROW should be a list of each column."""
        self.data.append(Row(values, annotations=annotations))

    def add_raw(self, val):
        self.data.append(Raw(val))

    def ncols(self):
        """ Return the number of maximum number of cols in the data """
        if self.data:
            return max([row.ncols() for row in self.data if type(row) == Row])
        return 0

    ################################################################

    def format_cell(self, value, col_number):
        """ Format a value that appears in a given colNumber. The first column Number is 0.
        Returns (value,alignment)
        """
        formatted_value = None
        if value is None:
            return "", self.LEFT
        if value == 0 and self.SUPPRESS_ZERO in self.options:
            return "", self.LEFT
        if isnumber(value):
            try:
                formatted_value = self.col_fmt.get(col_number, self.col_fmt_default).format(value)
                default_alignment = self.DEFAULT_ALIGNMENT_NUMBER
            except (ValueError, TypeError) as e:
                print(str(e))
                print("Format string: ", self.col_fmt.get(col_number, self.col_fmt_default))
                print("Value:         ", value)
                print("Will use default formatting")
                pass  # will be formatted below

        if not formatted_value:
            formatted_value = str(value)
            default_alignment = self.DEFAULT_ALIGNMENT_STRING
        return formatted_value, self.col_alignment.get(col_number, default_alignment)

    def col_formatted_width(self, col_num):
        """ Returns the width of column number colNum """
        max_col_width = 0
        for r in self.col_headings:
            try:
                max_col_width = max(max_col_width, len(self.format_cell(r[col_num], col_num)[0]))
            except IndexError:
                pass
        for r in self.data:
            try:
                max_col_width = max(max_col_width, len(self.format_cell(r[col_num], col_num)[0]))
            except IndexError:
                pass
        return max_col_width

    ################################################################

    def typeset_hr(self):
        """Output a HR."""
        if self.mode == self.LATEX:
            return "\\hline\n "
        elif self.mode == self.TEXT:
            return "+".join(["-" * self.col_formatted_width(col) for col in range(0, self.cols)]) + "\n"
        elif self.mode == self.HTML:
            return ""  # don't insert
        raise ValueError("Unknown mode '{}'".format(self.mode))

    def typeset_cell(self, formatted_value, col_number):
        """Typeset a value for a given column number."""
        import math
        align = self.col_alignment.get(col_number, self.LEFT)
        if self.mode == self.HTML:
            return formatted_value
        if self.mode == self.LATEX:
            if self.OPTION_NO_ESCAPE in self.options:
                return formatted_value
            else:
                return latex_tools.latex_escape(formatted_value)
        if self.mode == self.TEXT:
            try:
                fill = (self.col_formatted_widths[col_number] - len(formatted_value))
            except IndexError:
                fill = 0
            if align == self.RIGHT:
                return " " * fill + formatted_value
            if align == self.CENTER:
                return " " * math.ceil(fill / 2.0) + formatted_value + " " * math.floor(fill / 2.0)
            # Must be LEFT
            if col_number != self.cols - 1:  # not the last column
                return formatted_value + " " * fill
            return formatted_value  # don't indent last column

    def typeset_row(self, row, html_delim='td'):
        """row is a an array. It should be typeset. Return the string. """
        ret = []
        if isinstance(row, Raw):
            return row.data
        # if self.mode == self.HTML:
        #     return row.data
        if self.mode == self.HTML:
            ret.append("<tr>")
        for colNumber in range(0, len(row)):
            if colNumber > 0:
                if self.mode == self.LATEX:
                    ret.append(" & ")
                ret.append(" " * self.col_margin)
            (fmt, just) = self.format_cell(row[colNumber], colNumber)
            val = self.typeset_cell(fmt, colNumber)

            if self.mode == self.TEXT:
                ret.append(val)
            elif self.mode == self.LATEX:
                if row.annotations:
                    ret.append(row.annotations[colNumber])
                ret.append(val.replace('%', '\\%'))
            elif self.mode == self.HTML:
                ret.append(f'<{html_delim} {self.HTML_ALIGNMENT[just]}>{val}</{html_delim}>')
        if self.mode == self.HTML:
            ret.append("</tr>")
        ret.append(self.NL[self.mode])
        return "".join(ret)

    ################################################################

    def calculate_col_formatted_widths(self):
        """ Calculate the width of each formatted column and return the array """
        self.col_formatted_widths = []
        for i in range(0, self.cols):
            self.col_formatted_widths.append(self.col_formatted_width(i))
        return self.col_formatted_widths

    def should_omit_row(self, row):
        for (a, b) in self.omit_row:
            if row[a] == b:
                return True
        return False

    def compute_and_add_col_totals(self):
        """ Add totals for the specified cols"""
        self.cols = self.ncols()
        totals = [0] * self.cols
        try:
            for r in self.data:
                if self.should_omit_row(r):
                    continue
                if r == self.HR:
                    continue  # can't total HRs
                for col in self.col_totals:
                    if r[col] == '':
                        continue
                    totals[col] += r[col]
        except (ValueError, TypeError) as e:
            print("*** Table cannot be totaled", file=sys.stderr)
            for row in self.data:
                print(row.data, file=sys.stderr)
            raise e
        row = ["Total"]
        for col in range(1, self.cols):
            if col in self.col_totals:
                row.append(totals[col])
            else:
                row.append("")
        self.add_data(self.HR)
        self.add_data(row)
        self.add_data(self.HR)
        self.add_data(self.HR)

    ################################################################
    def typeset_headings(self):
        #
        # Typeset the headings
        #
        ret = []
        if self.col_headings:
            for heading_row in self.col_headings:
                ret.append(self.typeset_row(heading_row, html_delim='th'))
            for i in range(0, self.heading_hr_count):
                ret.append(self.typeset_hr())
        return ret

    def typeset(self, *, mode=None, option=None, out=None):
        """ Returns the typeset output of the entire table. Builds it up in """

        if (self.OPTION_LONGTABLE in self.options) and (self.OPTION_TABULARX in self.options):
            raise RuntimeError("OPTION_LONGTABLE and OPTION_TABULARX conflict")

        if len(self.data) == 0:
            print("typeset: no rows")
            return ""

        if mode:
            self.set_mode(mode)

        if self.mode not in [self.TEXT, self.LATEX, self.HTML]:
            raise ValueError("Invalid typesetting mode " + self.mode)

        if option:
            self.add_option(option)
            print("add option", option)
        self.cols = self.ncols()  # cache
        if self.cols == 0:
            print("typeset: no data")
            return ""

        if self.mode not in [self.TEXT, self.LATEX, self.HTML]:
            raise ValueError("Invalid typesetting mode " + self.mode)

        if self.mode not in [self.TEXT, self.LATEX, self.HTML]:
            raise ValueError("Invalid typesetting mode " + self.mode)

        ret = [""]  # array of strings that will be concatenatted

        # If we need column totals, compute them
        if hasattr(self, "col_totals"):
            self.compute_and_add_col_totals()

        # Precalc any table widths if necessary 
        if self.mode == self.TEXT:
            self.calculate_col_formatted_widths()
            if self.title:
                ret.append(self.title + ":" + "\n")

        #
        # Start of the table 
        #
        if self.mode == self.LATEX:
            if self.fontsize:
                ret.append("{\\fontsize{%d}{%d}\\selectfont" % (self.fontsize, self.fontsize + 1))
            try:
                colspec = self.latex_colspec
            except AttributeError:
                colspec = "r" * self.cols
            if self.OPTION_LONGTABLE not in self.options:
                # Regular table
                if self.OPTION_TABLE in self.options:
                    ret.append("\\begin{table}")
                if self.OPTION_CENTER in self.options:
                    ret.append("\\begin{center}")
                if self.caption:
                    ret += ["\\caption{", self.caption, "}\n"]
                if self.label:
                    ret.append("\\label{")
                    ret.append(self.label)
                    ret.append("}")
                if self.OPTION_TABULARX in self.options:
                    ret += ["\\begin{tabularx}{\\textwidth}{", colspec, "}\n"]
                else:
                    ret += ["\\begin{tabular}{", colspec, "}\n"]
                ret += self.typeset_headings()
            if self.OPTION_LONGTABLE in self.options:
                # Longtable
                ret += ["\\begin{longtable}{", colspec, "}\n"]
                if self.caption:
                    ret += ["\\caption{", self.caption, "}\\\\ \n"]
                if self.label:
                    ret += ["\\label{", self.label, "}"]
                ret += self.typeset_headings()
                ret.append("\\hline\\endfirsthead\n")
                if self.label:
                    ret += [r'\multicolumn{', str(self.ncols()), r'}{c}{(Table \ref{', self.label, r'} continued)}\\', '\n']
                ret += self.typeset_headings()
                ret.append("\\hline\\endhead\n")
                ret += ['\\multicolumn{', str(self.ncols()), '}{c}{(Continued on next page)}\\\\ \n']
                ret.append(self.footer)
                ret.append("\\hline\\endfoot\n")
                ret.append(self.footer)
                ret.append("\\hline\\hline\\endlastfoot\n")
        elif self.mode == self.HTML:
            ret.append("<table>\n")
            ret += self.typeset_headings()
        elif self.mode == self.TEXT:
            if self.caption:
                ret.append("================ {} ================\n".format(self.caption))
            if self.header:
                ret.append(self.header)
                ret.append("\n")
            ret += self.typeset_headings()

        #
        # typeset each row.
        # computes the width of each row if necessary
        #
        for row in self.data:

            # See if we should omit this row
            if self.should_omit_row(row):
                continue

            # See if this row demands special processing
            if row.data == self.HR:
                ret.append(self.typeset_hr())
                continue

            ret.append(self.typeset_row(row))

        #
        # End of the table
        ##

        if self.mode == self.LATEX:
            if self.OPTION_LONGTABLE not in self.options:
                if self.OPTION_TABULARX in self.options:
                    ret.append("\\end{tabularx}\n")
                else:
                    ret.append("\\end{tabular}\n")
                if self.OPTION_CENTER in self.options:
                    ret.append("\\end{center}")
                if self.OPTION_TABLE in self.options:
                    ret.append("\\end{table}")
            else:
                ret.append("\\end{longtable}\n")
            if self.fontsize:
                ret.append("}")
            if self.footnote:
                ret.append("\\footnote{")
                ret.append(latex_tools.latex_escape(self.footnote))
                ret.append("}")
        elif self.mode == self.HTML:
            ret.append("</table>\n")
        elif self.mode == self.TEXT:
            if self.footer:
                ret.append(self.footer)
                ret.append("\n")

        # Finally, add any variables that have been defined
        for (name, value) in self.variables.items():
            if self.mode == self.LATEX:
                ret += latex_var(name, value)
            if self.mode == self.HTML:
                ret += "".join(["Note: ", name, " is ", value, "<br>"])

        outbuffer = "".join(ret)
        if out:
            out.write(outbuffer)
        return outbuffer

    def add_variable(self, name, value):
        self.variables[name] = value

    def save_table(self, fname, mode=LATEX, option=None):
        with open(fname, "w") as f:
            f.write(self.typeset(mode=mode, option=option))

    def add_sql(self, db, stmt, headings=None, footnote=False):
        if footnote:
            self.footnote = stmt
        cur = db.cursor()
        try:
            cur.execute(stmt)
        except sqlite3.OperationalError:
            raise RuntimeError("Invalid SQL statement: " + stmt)
        if headings:
            self.add_head(headings)
        else:
            self.add_head([col[0] for col in cur.description])
        [self.add_data(row) for row in cur]


def demo():
    doc = tytable()
    doc.add_head(['State', 'Abbreviation', 'Population'])
    doc.add_data(['Virginia', 'VA', 8001045])
    doc.add_data(['California', 'CA', 37252895])
    return doc


if __name__ == "__main__":
    # Showcase different ways of making a document and render it each way:
    doc: tytable = demo()
    print(doc.prettyprint())
    exit(0)
