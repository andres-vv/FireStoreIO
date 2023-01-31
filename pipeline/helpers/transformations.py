import json
import math
import logging

import apache_beam as beam


class LevelUp(beam.DoFn):
    """Class to level up a Bulbasaur, adjust stats and evolve if needed."""

    def __init__(self, gained_levels, pokemon):
        self.gained_levels = int(gained_levels)
        self.pokemon = pokemon
        self.base_stats = {
            'HP': 45,
            'Atk': 49,
            'Def': 49,
            'Sp. Atk': 65,
            'Sp. Def': 65,
            'Speed': 45
        }

    def update_stats(self, element):
        """Update stats based on gained levels."""
        elvolve = 1
        if int(element['Level']) >= 16:
            element['Name'] = 'Ivysaur'
            evolve = 1.5
        if int(element['Level']) >= 32:
            element['Name'] = 'Venusaur'
            evolve = 2.5

        # Update HP
        element['HP'] = int((math.floor(
            0.01 * 2 *
            self.base_stats['HP'] * int(element['Level'])) +
            int(element['Level']) + 10) * evolve)
        # Update other stats
        for key, value in element.items():
            if key in ['Atk', 'Def', 'Sp. Atk', 'Sp. Def', 'Speed']:
                element[key] = int(math.floor(
                    0.01 * 2 * self.base_stats[key] * int(element['Level']) + 5) * evolve)

        return element

    def process(self, element):
        element = json.loads(element)
        logging.info(element)
        if element['Name'] == self.pokemon:
            element['Level'] = int(element['Level']) + self.gained_levels
            element = self.update_stats(element)
        yield str(json.dumps(element))
