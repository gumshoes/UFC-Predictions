import json
import os
import pickle
from typing import Dict, List

import requests
import csv
import pandas as pd
from bs4 import BeautifulSoup

from src.createdata.scrape_fight_links import UFCLinks
from src.createdata.utils import make_soup, print_progress

from src.createdata.data_files_path import (  # isort:skip
    NEW_EVENT_AND_FIGHTS,
    TOTAL_EVENT_AND_FIGHTS,
    FIGHTMETRIC_EVENTS_PICKLE,
    FIGHTMETRIC_FIGHTS_PICKLE,
)


class FightDataScraperV2:
    def __init__(self):
        self.HEADER: str = "R_fighter;B_fighter;R_KD;B_KD;R_SIG_STR.;B_SIG_STR.\
        ;R_SIG_STR_pct;B_SIG_STR_pct;R_TOTAL_STR.;B_TOTAL_STR.;R_TD;B_TD;R_TD_pct\
        ;B_TD_pct;R_SUB_ATT;B_SUB_ATT;R_REV;B_REV;R_CTRL;B_CTRL;R_HEAD;B_HEAD;R_BODY\
        ;B_BODY;R_LEG;B_LEG;R_DISTANCE;B_DISTANCE;R_CLINCH;B_CLINCH;R_GROUND;B_GROUND\
        ;win_by;last_round;last_round_time;Format;Referee;date;location;Fight_type;Winner\n"

        self.fight_data: dict = {}
        self.event_data: dict = {}

    def __enter__(self):
        # Load cached data (it will be persisted at class exit).
        if FIGHTMETRIC_FIGHTS_PICKLE.exists():
            with open(FIGHTMETRIC_FIGHTS_PICKLE.as_posix(), 'rb') as pickle_in:
                self.fight_data = pickle.load(pickle_in)

        if FIGHTMETRIC_EVENTS_PICKLE.exists():
            with open(FIGHTMETRIC_EVENTS_PICKLE.as_posix(), 'rb') as pickle_in:
                self.event_data = pickle.load(pickle_in)

        return self

    def get_fightmetric_event_data(self, event_id):
        event_json = None

        if event_id in self.event_data:
            event_json = self.event_data[event_id]
        else:
            try:
                url_event = f'http://liveapiorigin.fightmetric.com/V1/{event_id}/Fnt.json'
                print(url_event)
                r = requests.get(url_event)
                if r.ok:
                    event_json = r.json()
                    self.event_data[event_id] = event_json
                else:
                    print(r.status_code)
            except Exception as e:
                pass

        return event_json

    def get_fightmetric_fight_data(self, event_id, fight_id):
        fight_json = None
        comb_id = f'{event_id}_{fight_id}'

        if comb_id in self.fight_data:
            fight_json = self.fight_data[comb_id]
        else:
            try:
                url_fight = f'http://liveapiorigin.fightmetric.com/V2/{event_id}/{fight_id}/Stats.json'
                print(url_fight)
                r = requests.get(url_fight)
                if r.ok:
                    fight_json = r.json()
                    self.fight_data[comb_id] = fight_json
                else:
                    print(r.status_code)

            except Exception as e:
                pass

        return fight_json

    def create_fight_data_csv(self) -> None:
        print("Scraping data.")

        csv_columns = ['event_id', 'date', 'gmt', 'venue', 'city', 'country', 'fight_id', 'weight_class', 'status',
                       'possible_rounds', 'final_round',
                       'last_round_time', 'win_by',
                       'R_fighter', 'B_fighter', 'R_fighter_id', 'B_fighter_id', 'R_outcome', 'B_outcome',
                       'R_knock_down', 'B_knock_down',
                       'R_standups', 'B_standups', 'R_takedowns_attempts', 'R_takedowns_landed', 'B_takedowns_attempts',
                       'B_takedowns_landed',
                       'R_submissions_attempts', 'B_submissions_attempts', 'R_reversals_landed', 'B_reversals_landed',
                       'R_sig_str_attempts', 'R_sig_str_landed', 'B_sig_str_attempts', 'B_sig_str_landed',
                       'R_total_str_attempts', 'R_total_str_landed',
                       'B_total_str_attempts', 'B_total_str_landed',
                       'R_distance_str_attempts', 'R_distance_str_landed',
                       'B_distance_str_attempts', 'B_distance_str_landed',
                       'R_clinch_sig_str_attempts', 'R_clinch_sig_str_landed',
                       'B_clinch_sig_str_attempts', 'B_clinch_sig_str_landed',
                       'R_ground_sig_str_attempts', 'R_ground_sig_str_landed',
                       'B_ground_sig_str_attempts', 'B_ground_sig_str_landed',
                       'R_clinch_total_str_attempts', 'R_clinch_total_str_landed',
                       'B_clinch_total_str_attempts', 'B_clinch_total_str_landed',
                       'R_ground_total_str_attempts', 'R_ground_total_str_landed',
                       'B_ground_total_str_attempts', 'B_ground_total_str_landed',
                       'R_head_sig_str_attempts', 'R_head_sig_str_landed',
                       'B_head_sig_str_attempts', 'B_head_sig_str_landed',
                       'R_body_sig_str_attempts', 'R_body_sig_str_landed',
                       'B_body_sig_str_attempts', 'B_body_sig_str_landed',
                       'R_legs_sig_str_attempts', 'R_legs_sig_str_landed',
                       'B_legs_sig_str_attempts', 'B_legs_sig_str_landed',

                       'R_head_total_str_attempts', 'R_head_total_str_landed',
                       'B_head_total_str_attempts', 'B_head_total_str_landed',
                       'R_body_total_str_attempts', 'R_body_total_str_landed',
                       'B_body_total_str_attempts', 'B_body_total_str_landed',
                       'R_legs_total_str_attempts', 'R_legs_total_str_landed',
                       'B_legs_total_str_attempts', 'B_legs_total_str_landed',

                       'R_distance_head_str_attempts', 'R_distance_head_str_landed',
                       'B_distance_head_str_attempts', 'B_distance_head_str_landed',
                       'R_distance_body_str_attempts', 'R_distance_body_str_landed',
                       'B_distance_body_str_attempts', 'B_distance_body_str_landed',
                       'R_distance_leg_str_attempts', 'R_distance_leg_str_landed',
                       'B_distance_leg_str_attempts', 'B_distance_leg_str_landed',
                       'R_clinch_head_str_attempts', 'R_clinch_head_str_landed',
                       'B_clinch_head_str_attempts', 'B_clinch_head_str_landed',
                       'R_clinch_body_str_attempts', 'R_clinch_body_str_landed',
                       'B_clinch_body_str_attempts', 'B_clinch_body_str_landed',
                       'R_clinch_leg_str_attempts', 'R_clinch_leg_str_landed',
                       'B_clinch_leg_str_attempts', 'B_clinch_leg_str_landed',
                       'R_ground_head_str_attempts', 'R_ground_head_str_landed',
                       'B_ground_head_str_attempts', 'B_ground_head_str_landed',
                       'R_ground_body_str_attempts', 'R_ground_body_str_landed',
                       'B_ground_body_str_attempts', 'B_ground_body_str_landed',
                       'R_ground_leg_str_attempts', 'R_ground_leg_str_landed',
                       'B_ground_leg_str_attempts', 'B_ground_leg_str_landed',

                       'R_standing_time', 'B_standing_time', 'R_control_time', 'B_control_time', 'R_ground_time',
                       'B_ground_time',
                       'R_neutral_time', 'B_neutral_time', 'R_ground_control_time', 'B_ground_control_time',
                       'R_distance_time', 'B_distance_time',
                       'R_clinch_time', 'B_clinch_time']
        out_rows = []

        # TODO: determine this range by using https://fightmetric.rds.ca/events/completed to find the most recent completed?
        for event_id in range(900, 1005):
            try:
                event_json = self.get_fightmetric_event_data(event_id)
                event = event_json['FMLiveFeed']

                for fight in event['Fights']:
                    x = {'event_id': event['EventID'], 'date': event['Date'], 'fight_id': fight['FightID'],
                         'weight_class': fight['WeightClassName'],
                         'status': fight['Status'], 'possible_rounds': fight['PossibleRds'],
                         'final_round': fight['EndingRoundNum'],
                         'win_by': fight['Method'], 'gmt': event['GMT'], 'venue': event['Venue'],
                         'country': event['Country'], 'city': event['City']}

                    for f in fight['Fighters']:
                        clr = f['Color']
                        clr_prefix = 'R'
                        if clr == 'Blue':
                            clr_prefix = 'B'
                        elif clr == 'Red':
                            clr_prefix = 'R'
                        x[f'{clr_prefix}_fighter'] = f['FullName']
                        x[f'{clr_prefix}_fighter_id'] = f['FighterID']
                        x[f'{clr_prefix}_outcome'] = f['Outcome']

                    fight_json = self.get_fightmetric_fight_data(event_id, fight['FightID'])
                    fight_stats = fight_json['FMLiveFeed']['FightStats']
                    x['last_round_time'] = fight_json['FMLiveFeed']['CurrentRoundTime']

                    for clr in ['Red', 'Blue']:
                        clr_prefix = 'R'
                        if clr == 'Blue':
                            clr_prefix = 'B'
                        elif clr == 'Red':
                            clr_prefix = 'R'
                        grp = fight_stats[clr]['Grappling']
                        x[f'{clr_prefix}_standups'] = grp['Standups']['Landed']
                        x[f'{clr_prefix}_takedowns_attempts'] = grp['Takedowns']['Attempts']
                        x[f'{clr_prefix}_takedowns_landed'] = grp['Takedowns']['Landed']
                        x[f'{clr_prefix}_submissions_attempts'] = grp['Submissions']['Attempts']
                        x[f'{clr_prefix}_reversals_landed'] = grp['Reversals']['Landed']

                        strikes = fight_stats[clr]['Strikes']
                        x[f'{clr_prefix}_knock_down'] = strikes['Knock Down']['Landed']
                        x[f'{clr_prefix}_sig_str_attempts'] = strikes['Significant Strikes']['Attempts']
                        x[f'{clr_prefix}_sig_str_landed'] = strikes['Significant Strikes']['Landed']
                        x[f'{clr_prefix}_total_str_attempts'] = strikes['Total Strikes']['Attempts']
                        x[f'{clr_prefix}_total_str_landed'] = strikes['Total Strikes']['Landed']

                        x[f'{clr_prefix}_distance_str_attempts'] = strikes['Distance Strikes']['Attempts']
                        x[f'{clr_prefix}_distance_str_landed'] = strikes['Distance Strikes']['Landed']
                        x[f'{clr_prefix}_clinch_sig_str_attempts'] = strikes['Clinch Significant Strikes']['Attempts']
                        x[f'{clr_prefix}_clinch_sig_str_landed'] = strikes['Clinch Significant Strikes']['Landed']
                        x[f'{clr_prefix}_ground_sig_str_attempts'] = strikes['Ground Significant Strikes']['Attempts']
                        x[f'{clr_prefix}_ground_sig_str_landed'] = strikes['Ground Significant Strikes']['Landed']
                        x[f'{clr_prefix}_clinch_total_str_attempts'] = strikes['Clinch Total Strikes']['Attempts']
                        x[f'{clr_prefix}_clinch_total_str_landed'] = strikes['Clinch Total Strikes']['Landed']
                        x[f'{clr_prefix}_ground_total_str_attempts'] = strikes['Ground Total Strikes']['Attempts']
                        x[f'{clr_prefix}_ground_total_str_landed'] = strikes['Ground Total Strikes']['Landed']
                        x[f'{clr_prefix}_head_sig_str_attempts'] = strikes['Head Significant Strikes']['Attempts']
                        x[f'{clr_prefix}_head_sig_str_landed'] = strikes['Head Significant Strikes']['Landed']
                        x[f'{clr_prefix}_body_sig_str_attempts'] = strikes['Body Significant Strikes']['Attempts']
                        x[f'{clr_prefix}_body_sig_str_landed'] = strikes['Body Significant Strikes']['Landed']
                        x[f'{clr_prefix}_legs_sig_str_attempts'] = strikes['Legs Significant Strikes']['Attempts']
                        x[f'{clr_prefix}_legs_sig_str_landed'] = strikes['Legs Significant Strikes']['Landed']
                        x[f'{clr_prefix}_head_total_str_attempts'] = strikes['Head Total Strikes']['Attempts']
                        x[f'{clr_prefix}_head_total_str_landed'] = strikes['Head Total Strikes']['Landed']
                        x[f'{clr_prefix}_body_total_str_attempts'] = strikes['Body Total Strikes']['Attempts']
                        x[f'{clr_prefix}_body_total_str_landed'] = strikes['Body Total Strikes']['Landed']
                        x[f'{clr_prefix}_legs_total_str_attempts'] = strikes['Legs Total Strikes']['Attempts']
                        x[f'{clr_prefix}_legs_total_str_landed'] = strikes['Legs Total Strikes']['Landed']
                        x[f'{clr_prefix}_distance_head_str_attempts'] = strikes['Distance Head Strikes']['Attempts']
                        x[f'{clr_prefix}_distance_head_str_landed'] = strikes['Distance Head Strikes']['Landed']
                        x[f'{clr_prefix}_distance_body_str_attempts'] = strikes['Distance Body Strikes']['Attempts']
                        x[f'{clr_prefix}_distance_body_str_landed'] = strikes['Distance Body Strikes']['Landed']
                        x[f'{clr_prefix}_distance_leg_str_attempts'] = strikes['Distance Leg Strikes']['Attempts']
                        x[f'{clr_prefix}_distance_leg_str_landed'] = strikes['Distance Leg Strikes']['Landed']
                        x[f'{clr_prefix}_clinch_head_str_attempts'] = strikes['Clinch Head Strikes']['Attempts']
                        x[f'{clr_prefix}_clinch_head_str_landed'] = strikes['Clinch Head Strikes']['Landed']
                        x[f'{clr_prefix}_clinch_body_str_attempts'] = strikes['Clinch Body Strikes']['Attempts']
                        x[f'{clr_prefix}_clinch_body_str_landed'] = strikes['Clinch Body Strikes']['Landed']
                        x[f'{clr_prefix}_clinch_leg_str_attempts'] = strikes['Clinch Leg Strikes']['Attempts']
                        x[f'{clr_prefix}_clinch_leg_str_landed'] = strikes['Clinch Leg Strikes']['Landed']
                        x[f'{clr_prefix}_ground_head_str_attempts'] = strikes['Ground Head Strikes']['Attempts']
                        x[f'{clr_prefix}_ground_head_str_landed'] = strikes['Ground Head Strikes']['Landed']
                        x[f'{clr_prefix}_ground_body_str_attempts'] = strikes['Ground Body Strikes']['Attempts']
                        x[f'{clr_prefix}_ground_body_str_landed'] = strikes['Ground Body Strikes']['Landed']
                        x[f'{clr_prefix}_ground_leg_str_attempts'] = strikes['Ground Leg Strikes']['Attempts']
                        x[f'{clr_prefix}_ground_leg_str_landed'] = strikes['Ground Leg Strikes']['Landed']

                        tip = fight_stats[clr]['TIP']
                        x[f'{clr_prefix}_standing_time'] = tip['Standing Time']
                        x[f'{clr_prefix}_control_time'] = tip['Control Time']
                        x[f'{clr_prefix}_ground_time'] = tip['Ground Time']
                        x[f'{clr_prefix}_neutral_time'] = tip['Neutral Time']
                        x[f'{clr_prefix}_ground_control_time'] = tip['Ground Control Time']
                        x[f'{clr_prefix}_distance_time'] = tip['Distance Time']
                        x[f'{clr_prefix}_clinch_time'] = tip['Clinch Time']

                    out_rows.append(x)
            except Exception as e:
                print(e)

        with open('data/fightmetric.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in out_rows:
                writer.writerow(data)

    def __exit__(self, exc_type, exc_val, exc_tb):
        with open(FIGHTMETRIC_FIGHTS_PICKLE.as_posix(), "wb") as f:
            pickle.dump(self.fight_data, f)

        with open(FIGHTMETRIC_EVENTS_PICKLE.as_posix(), "wb") as f:
            pickle.dump(self.event_data, f)
        pass


class FightDataScraper:
    def __init__(self):
        self.HEADER: str = "R_fighter;B_fighter;R_KD;B_KD;R_SIG_STR.;B_SIG_STR.\
;R_SIG_STR_pct;B_SIG_STR_pct;R_TOTAL_STR.;B_TOTAL_STR.;R_TD;B_TD;R_TD_pct\
;B_TD_pct;R_SUB_ATT;B_SUB_ATT;R_REV;B_REV;R_CTRL;B_CTRL;R_HEAD;B_HEAD;R_BODY\
;B_BODY;R_LEG;B_LEG;R_DISTANCE;B_DISTANCE;R_CLINCH;B_CLINCH;R_GROUND;B_GROUND\
;win_by;last_round;last_round_time;Format;Referee;date;location;Fight_type;Winner\n"

        self.NEW_EVENT_AND_FIGHTS_PATH = NEW_EVENT_AND_FIGHTS
        self.TOTAL_EVENT_AND_FIGHTS_PATH = TOTAL_EVENT_AND_FIGHTS

    def create_fight_data_csv(self) -> None:
        print("Scraping links!")

        ufc_links = UFCLinks()
        new_events_and_fight_links, all_events_and_fight_links = (
            ufc_links.get_event_and_fight_links()
        )
        print("Successfully scraped and saved event and fight links!\n")
        print("Now, scraping event and fight data!\n")

        if not new_events_and_fight_links:
            if self.TOTAL_EVENT_AND_FIGHTS_PATH.exists():
                print("No new fight data to scrape at the moment!")
                return
            else:
                self._scrape_raw_fight_data(
                    all_events_and_fight_links,
                    filepath=self.TOTAL_EVENT_AND_FIGHTS_PATH,
                )
        else:
            self._scrape_raw_fight_data(
                new_events_and_fight_links, filepath=self.NEW_EVENT_AND_FIGHTS_PATH
            )

            new_event_and_fights_data = pd.read_csv(self.NEW_EVENT_AND_FIGHTS_PATH)
            old_event_and_fights_data = pd.read_csv(self.TOTAL_EVENT_AND_FIGHTS_PATH)

            assert len(new_event_and_fights_data.columns) == len(
                old_event_and_fights_data.columns
            )

            new_event_and_fights_data = new_event_and_fights_data[
                list(old_event_and_fights_data.columns)
            ]

            latest_total_fight_data = new_event_and_fights_data.append(
                old_event_and_fights_data, ignore_index=True
            )
            latest_total_fight_data.to_csv(self.TOTAL_EVENT_AND_FIGHTS_PATH, index=None)

            os.remove(self.NEW_EVENT_AND_FIGHTS_PATH)
            print("Removed new event and fight files")

        print("Successfully scraped and saved ufc fight data!\n")

    def _scrape_raw_fight_data(
            self, event_and_fight_links: Dict[str, List[str]], filepath
    ):
        if filepath.exists():
            print("file already exists. Overwriting!")

        total_stats = FightDataScraper._get_total_fight_stats(event_and_fight_links)
        with open(filepath.as_posix(), "wb") as file:
            file.write(bytes(self.HEADER, encoding="ascii", errors="ignore"))
            file.write(bytes(total_stats, encoding="ascii", errors="ignore"))

    @classmethod
    def _get_total_fight_stats(cls, event_and_fight_links: Dict[str, List[str]]) -> str:
        total_stats = ""

        l = len(event_and_fight_links)
        print("Scraping all fight data: ")
        print_progress(0, l, prefix="Progress:", suffix="Complete")

        for index, (event, fights) in enumerate(event_and_fight_links.items()):
            event_soup = make_soup(event)
            event_info = FightDataScraper._get_event_info(event_soup)

            for fight in fights:
                try:
                    fight_soup = make_soup(fight)
                    fight_stats = FightDataScraper._get_fight_stats(fight_soup)
                    fight_details = FightDataScraper._get_fight_details(fight_soup)
                    result_data = FightDataScraper._get_fight_result_data(fight_soup)
                except Exception as e:
                    continue

                total_fight_stats = (
                        fight_stats
                        + ";"
                        + fight_details
                        + ";"
                        + event_info
                        + ";"
                        + result_data
                )

                if total_stats == "":
                    total_stats = total_fight_stats
                else:
                    total_stats = total_stats + "\n" + total_fight_stats

            print_progress(index + 1, l, prefix="Progress:", suffix="Complete")

        return total_stats

    @classmethod
    def _get_fight_stats(cls, fight_soup: BeautifulSoup) -> str:
        tables = fight_soup.findAll("tbody")
        total_fight_data = [tables[0], tables[2]]
        fight_stats = []
        for table in total_fight_data:
            row = table.find("tr")
            stats = ""
            for data in row.findAll("td"):
                if stats == "":
                    stats = data.text
                else:
                    stats = stats + "," + data.text
            fight_stats.append(
                stats.replace("  ", "")
                    .replace("\n\n", "")
                    .replace("\n", ",")
                    .replace(", ", ",")
                    .replace(" ,", ",")
            )

        fight_stats[1] = ";".join(fight_stats[1].split(",")[6:])
        fight_stats[0] = ";".join(fight_stats[0].split(","))
        fight_stats = ";".join(fight_stats)
        return fight_stats

    @classmethod
    def _get_fight_details(cls, fight_soup: BeautifulSoup) -> str:
        columns = ""
        for div in fight_soup.findAll("div", {"class": "b-fight-details__content"}):
            for col in div.findAll("p", {"class": "b-fight-details__text"}):
                if columns == "":
                    columns = col.text
                else:
                    columns = columns + "," + (col.text)

        columns = (
            columns.replace("  ", "")
                .replace("\n\n\n\n", ",")
                .replace("\n", "")
                .replace(", ", ",")
                .replace(" ,", ",")
                .replace("Method: ", "")
                .replace("Round:", "")
                .replace("Time:", "")
                .replace("Time format:", "")
                .replace("Referee:", "")
        )

        fight_details = ";".join(columns.split(",")[:5])

        return fight_details

    @classmethod
    def _get_event_info(cls, event_soup: BeautifulSoup) -> str:
        event_info = ""
        for info in event_soup.findAll("li", {"class": "b-list__box-list-item"}):
            if event_info == "":
                event_info = info.text
            else:
                event_info = event_info + ";" + info.text

        event_info = ";".join(
            event_info.replace("Date:", "")
                .replace("Location:", "")
                .replace("Attendance:", "")
                .replace("\n", "")
                .replace("  ", "")
                .split(";")[:2]
        )

        return event_info

    @classmethod
    def _get_fight_result_data(cls, fight_soup: BeautifulSoup) -> str:
        winner = ""
        for div in fight_soup.findAll("div", {"class": "b-fight-details__person"}):
            if (
                    div.find(
                        "i",
                        {
                            "class": "b-fight-details__person-status b-fight-details__person-status_style_green"
                        },
                    )
                    is not None
            ):
                winner = (
                    div.find("h3", {"class": "b-fight-details__person-name"})
                        .text.replace(" \n", "")
                        .replace("\n", "")
                )

        fight_type = (
            fight_soup.find("i", {"class": "b-fight-details__fight-title"})
                .text.replace("  ", "")
                .replace("\n", "")
        )

        return fight_type + ";" + winner
