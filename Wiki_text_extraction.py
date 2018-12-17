'''

IMPORTS

'''
import os
import sys
import csv
import codecs
import re
import numpy as np
import pickle
import h5py

from tqdm import tqdm
from bs4 import BeautifulSoup as Soup
from collections import defaultdict
import xml.etree.ElementTree as etree

if sys.platform == 'win32':
    raise NotImplementedError

elif sys.platform == 'darwin':
    homedir = os.path.expanduser('~/')
    libra = 'Desktop/LIBRARIES/'
else:
    raise ValueError('YOUR SYSTEM IS NOT SUPPORTED!')


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
'''

G E N E R A L   N O T E S

(A) THIS MODULE SERVES TO HOLD FUNCTIONS AND CLASSES FOR EXTRACTION INFORMATION FORM WIKIPEDIA

'''


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
'''

H E L P E R   F U N C T I O N S

'''


def _strip_tag_name(tag: "tag in parsed xml tree element")-> str:
    """
    -----------------------------------------------------------------------

    ::: HELPER FUNCTION ::: strip_tag_name, this function strips the tag name
    from tag and returns it as string.

    -----------------------------------------------------------------------

    [] DESCRIPTION [] :::

        ~~~ USAGE EXAMPLE PATH ~~~
                            ~~~ LOCAL ::: THIS CLASS

    <> PARAMETERS <>
                  <> tag = "tag"

    >< RETURNS ><
                ><  str

    ::: NOTES :::
    """

    # -------- CODE -------------------------------------------------------
    idx = tag.rfind("}")

    if idx != -1:
        tag = tag[idx + 1:]

    return tag


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
'''

M A I N   F U N C T I O N S

'''


def create_main_articles_and_redirect_tables(wiki_xml_source_path: str,
                                             file_name_wiki: str,
                                             parsed_data_dump_path: str,
                                             unique_id: str = 'test'):
    """
    -----------------------------------------------------------------------

    ::: MAIN FUNCTION ::: create_main_articles_and_redirect_tables,
                    This saves tables into corresponding .csv tables:

                    table 1: is the table of redirections :: some texts with id's
                    are actually just redirections to the main paper

                    table 2: are actual main papers

                    table 3: the template tables :: id of the articles that are templates (wiki stubs?)

    -----------------------------------------------------------------------

    [] DESCRIPTION [] :::

        ~~~ USAGE EXAMPLE PATH ~~~
                            ~~~ LOCAL ::: THIS CLASS

    <> PARAMETERS <>
                  <> wiki_xml_source_path = str : is the path to wikipedia xml data
                  <> file_name_wiki = str: is the name of wikipedia xml file ::: an actual xml file
                  <> parsed_data_dump_path = str : is the path where to save table 1 and table 2, table 3 from function description
                  <> unique_id = str : string with the unique id describing the post fix unique to given tables


    >< RETURNS ><
                ><  saves tables

    ::: NOTES :::
    """

    # -------- DEFAULTS --------------------------------------------------------
    pathWikiXML = os.path.join(wiki_xml_source_path, file_name_wiki)
    pathArticles = os.path.join(wiki_xml_source_path, "redirected_articles_%s.csv" % (unique_id))

    pathArticlesRedirect = os.path.join(PATH_WIKI_XML, "main_articles_%s.csv" % (unique_id))
    pathTemplateRedirect = os.path.join(PATH_WIKI_XML, "template_articles_%s.csv" % (unique_id))

    # -------- CODE --------------------------------------------------------
    with codecs.open(pathArticles, "w", ENCODING) as articlesFH, \
            codecs.open(pathArticlesRedirect, "w", ENCODING) as redirectFH, \
            codecs.open(pathTemplateRedirect, "w", ENCODING) as templateFH:

        '''
        (I) THIS OPENS UP WRITERS
        '''
        articlesWriter = csv.writer(articlesFH, quoting=csv.QUOTE_MINIMAL)
        redirectWriter = csv.writer(redirectFH, quoting=csv.QUOTE_MINIMAL)
        templateWriter = csv.writer(templateFH, quoting=csv.QUOTE_MINIMAL)

        '''
        (II) CREATE THE TABLES STRUCTURES
        '''
        articlesWriter.writerow(['id', 'title', 'redirect'])
        redirectWriter.writerow(['id', 'title', 'redirect'])
        templateWriter.writerow(['id', 'title'])

        for event, elem in etree.iterparse(pathWikiXML, events=('start', 'end')):
            '''
            (III) STRIP THE TAG NAME
            '''
            tname = _strip_tag_name(elem.tag)

            '''
            (IV) FOLLOWING CODE STRIPS VARIOUS DATA AND IF APPROPRIATE DUMPS
                THEM INTO THE OPEN fileWriter
            '''
            if event == 'start':

                if tname == 'page':
                    title, ind, redirect, inrevision, ns = '', -1, '', False, 0

                elif tname == 'revision':
                    # Do not pick up on revision id's
                    inrevision = True

            else:

                if tname == 'title':
                    title = elem.text

                elif tname == 'id' and not inrevision:
                    ind = int(elem.text)

                elif tname == 'redirect':
                    redirect = elem.attrib['title']

                elif tname == 'ns':
                    ns = int(elem.text)

                elif tname == 'page':
                    totalCount += 1

                    if ns == 10:
                        templateCount += 1
                        templateWriter.writerow([id, title])

                    elif len(redirect) > 0:
                        articleCount += 1
                        articlesWriter.writerow([id, title, redirect])

                    else:
                        redirectCount += 1
                        redirectWriter.writerow([id, title, redirect])

            elem.clear()


def find_text_id_and_raw_text(wiki_xml_source_path: str,
                              file_name_wiki: str,
                              parsed_data_dump_path: str,
                              unique_id: str = 'test'):
    """
    -----------------------------------------------------------------------

    ::: MAIN FUNCTION :::
                        ::: this function returns the dictionary of:
                        article_id --> find_text. This unction also saves the
                        dictionary as csv file
    -----------------------------------------------------------------------

    [] DESCRIPTION [] :::

        ~~~ USAGE EXAMPLE PATH ~~~
                            ~~~ LOCAL ::: THIS CLASS

    <> PARAMETERS <>
                  <> wiki_xml_source_path = str : is the path to wikipedia xml data
                  <> file_name_wiki = str: is the name of wikipedia xml file ::: an actual xml file
                  <> parsed_data_dump_path = str : is the path where to save dictinary from function description as csv file.
                  <> unique_id = str : string with the unique id describing the post fix unique to given tables


    >< RETURNS ><
                ><  saves table and returns dictionary

    ::: NOTES :::
    """
    # -------- DEFAULTS --------------------------------------------------------
    path_to_future_table = os.path.join(wiki_xml_source_path, "text_table_%s.csv" % (unique_id))

    # -------- CODE --------------------------------------------------------
    text, ind = defaultdict(lambda: []), -1

    for event, elem in etree.iterparse(pathWikiXML, events=('start', 'end')):

        '''
        (I) RECREATE THE TAG NAME
        '''
        tname = _strip_tag_name(elem.tag)

        if ind != -1 and elem.text:

            text[ind] += [elem.text]

        '''
        (II) YOU NEED TO RECOVER GOOD ID
        '''
        if event == 'start':

            if tname == 'page':
                title, ind, redirect, inrevision, ns = '', -1, '', False, 0

            elif tname == 'revision':
                # Do not pick up on revision id's
                inrevision = True
            else:
                pass

        else:
            if tname == 'title':
                title = elem.text
            elif tname == 'id' and not inrevision:
                ind = int(elem.text)
            elif tname == 'redirect':
                redirect = elem.attrib['title']
            else:
                pass

    '''
    (III) HAVING THE text DICTIONARY CREATED CREATE THE pd.DataFrame AND DUMP IT TO LOCAL DRIVE
    '''
    text_table = pd.DataFrame.from_dict(text, orient='index')
    text_table = text_table.reset_index()

    '''
    (IV) RESET INDICES AND CHANGE NAMES OF THE COLUMNS
    '''
    new_col_names = [str(mmbr) for mmbr in ['paper_id'] + list(text_table.columns)[1:]]

    # NOTE: from the current structure of WIKI dump and the way how I am creating the table :: THE 20-th column is the main text
    #
    # NOTE: there is much more information actually parsed :: I do not care about that extra info now
    new_col_names[20] = 'main_text'

    text_table.columns = new_col_names
    text_table.to_csv(path_to_future_table)

    '''
    (V) RETURN THE DICTIONARY
    '''
    return text


def parse_wiki_categories(path_to_data: str,
                          name_of_wiki_corpus: str,
                          path_to_save: str,
                          name_to_save: str):
    """
    -----------------------------------------------------------------------

    ::: parse_wiki_categories :::

    -----------------------------------------------------------------------

    [] DESCRIPTION [] :::

        ~~~ USAGE EXAMPLE PATH ~~~
                            ~~~ LOCAL ::: THIS CLASS

    <> PARAMETERS <>
                <> path_to_data: str = is full path to file of wiki data
                <> name_of_wiki_corpus: str =  name of enwiki xml corpus
                <> path_to_save: str = full path to directory where to save result
                <> name_to_save: str = name_to_save.h5

    >< RETURNS ><
                ><
    ::: NOTES :::

    """

    # ------ CODE -----------------------------------------------------------
    """
    (I) READ IN THE XML FILE
    """
    with open(os.path.join(path_to_data, name_of_wiki_corpus), 'r') as handle:
        soup = Soup(handle, "xml")

    """
    (II) INITIALIZE THE h5 FILE WHERE WE WILL SAVE IT
    """
    hf = h5py.File(os.path.join(path_to_save, name_to_save), 'w', libver='latest')

    """
    (III) ITERATE THROUGH CORPUS AND FIND PAGE ID AND CATEGORY
    """
    raise NotImplementedError("THIS FUNCTION HAS YET NOT BEEN IMPLEMENTED")


def convert_csv_themes_to_DIC(path_to_csv_folder: str,
                              path_to_future_DIC_folder: str,
                              wiki_dump_time_stamp: str,
                              saving_type: type):
    """
    -----------------------------------------------------------------------

    ::: convert_csv_themes_to_DIC :::

    -----------------------------------------------------------------------

    [] DESCRIPTION [] ::: this function takes the folder where ONLY the csv files
                        ::: with wiki themes and article id's are kept and converts it to one h5 file
                        :::

                        ::: NOTE the themes csv files MUST HAVE the following structure AND in that particular filte THERE CAN NOT BE ANY OTHER CSV FILE:
                            :[b'wiki article index', b'theme_1', b'theme_2', ....]


    ~~~ USAGE EXAMPLE PATH ~~~
                            ~~~ LOCAL ::: THIS CLASS

    <> PARAMETERS <>
                    <> path_to_csv_folder : str = is the FULL PATH to the folder where the wikipedia articles parsed themes are stored
                    <> path_to_future_h5_folder : str = is the FULL PATH to the folder where you will store the themes h5 data file
                    <> wiki_dump_time_stamp : str = is the TIME STAMP of the version of wikipedia dump you are using ::: THE STRUCTURE MUST BE : `DD_Mon_YYYY`, for example `20_Sep_2018`
                    <> saving_type : type = is EITHER 'h5' OR dict


    >< RETURNS ><
                >< THIS FUNCTION SAVES IT INTO THE h5 WITH THE FOLLOWING STRUCTURE:
                    IF saving_type == 'h5' :: h5 file name: `"wiki_themes_{}.h5".format(DD_Mon_YYYY)`, for example `wiki_themes_20_Sep_2018.h5`
                    IF  saving_type == dict ::: pickled dictionary : `"wiki_themes_{}.pickle".format(DD_Mon_YYYY)`, for example `wiki_themes_20_Sep_2018.pickle`



    ::: NOTES :::

    """
    # ------- DEFAULTS --------------------------------------------------------
    _allowed_saving_types = {dict, 'h5'}

    if saving_type not in _allowed_saving_types:
        raise ValueError("PROVIDED SAVING TYPE IS NOT ALLOWED!")

    _default_name = "wikipedia_themes_%s.%s" % (
        wiki_dump_time_stamp, 'h5' if saving_type == 'h5' else 'pickle')

    # ------- CODE ------------------------------------------------------------
    """
    (I) INITIALIZE THE h5 FILE WHERE WE WILL SAVE IT
    """
    hf = h5py.File(os.path.join(path_to_future_DIC_folder, _default_name),
                   'w', libver='latest') if saving_type == 'h5' else {}

    """
    (II) ITERATE THROUGHT THE FILES AND FOR EACH FILE ITERATE THROUGH ROWS AND DUMP IT INTO hf
    """

    for file_csv in tqdm(filter(lambda x: x.name.split('.')[-1] == 'csv',
                                os.scandir(path_to_csv_folder))):

        with open(file_csv.path, 'r', encoding='ascii') as f:

            reader = csv.reader(f, delimiter=',')

            # READ LINE BY LINE
            for row in reader:

                try:
                    article_id = row[0].strip('b').strip(
                        '\'') if saving_type == 'h5' else str(row[0].strip('b').strip('\''))

                    themes = np.array(list(map(lambda x: x.strip('b').strip('\'').encode('ascii', 'ignore'), row[1:]))) if saving_type == 'h5' else\
                        list(map(lambda x: str(x.strip('b').strip('\'')), row[1:]))

                except (TypeError, ValueError) as e:
                    print("COULD NOT FIND article_id OR themes ::: GOT THIS : \n ",
                          article_id, " ", themes, "\n")
                    continue

                # OK NOW YOU HAVE TO DUMP IT
                if saving_type == 'h5':
                    hf.create_dataset('%s' % (article_id), data=themes)

                else:
                    hf[article_id] = themes

    """
    (III) IMPORTANT CLOSE hf
    """
    if saving_type == 'h5':
        hf.close()

    else:
        with open(os.path.join(path_to_future_DIC_folder, _default_name), 'wb') as handle:
            pickle.dump(hf, handle, protocol=pickle.HIGHEST_PROTOCOL)
