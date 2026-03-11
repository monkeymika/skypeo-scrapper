"""
scraper.py — Logique de scraping via Google Places API (New)

Endpoints utilisés :
  - Text Search  : POST https://places.googleapis.com/v1/places:searchText
  - Place Details : GET  https://places.googleapis.com/v1/places/{place_id}

Documentation officielle :
  https://developers.google.com/maps/documentation/places/web-service/text-search
"""

from __future__ import annotations  # annotations lazy → compatible Python 3.7+

import concurrent.futures
import csv
import io
import json
import os
import re
import smtplib
import threading
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

try:
    import dns.resolver as _dns_resolver
    _DNS_OK = True
except ImportError:
    _DNS_OK = False

try:
    import whois as _whois_lib
    _WHOIS_OK = True
except ImportError:
    _WHOIS_OK = False

try:
    from playwright.sync_api import sync_playwright as _sync_playwright
    _PLAYWRIGHT_OK = True
except ImportError:
    _PLAYWRIGHT_OK = False

# Chargement de la clé API depuis le fichier .env
load_dotenv()

# ── Constantes API ─────────────────────────────────────────────────────────────

PLACES_API_BASE = "https://places.googleapis.com/v1"

# Champs demandés lors du Text Search (impact sur la facturation Google)
TEXT_SEARCH_FIELDS = [
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.nationalPhoneNumber",
    "places.websiteUri",
    "places.rating",
    "places.userRatingCount",
    "places.types",
    "places.businessStatus",
    "nextPageToken",  # nécessaire pour la pagination
]

# Champs demandés pour les détails d'un lieu (appel séparé, plus coûteux)
DETAIL_FIELDS = [
    "id",
    "displayName",
    "formattedAddress",
    "nationalPhoneNumber",
    "internationalPhoneNumber",
    "websiteUri",
    "rating",
    "userRatingCount",
    "types",
    "businessStatus",
]

# Colonnes du fichier CSV exporté (recherche rapide)
CSV_COLUMNS = [
    "nom",
    "adresse",
    "telephone",
    "email",
    "site_web",
    "note_google",
    "nombre_avis",
    "statut",
    "types",
]

# ── Constantes pour la collecte massive ────────────────────────────────────────

SECTORS = {
    "restauration": {
        "label": "Restauration",
        "icon": "🍽️",
        "types": ["restaurant", "cafe", "bar", "fast_food", "meal_takeaway"],
    },
    "beaute": {
        "label": "Beauté & Spa",
        "icon": "💆",
        "types": ["beauty_salon", "spa", "hair_care", "hair_salon", "nail_salon"],
    },
    "hotellerie": {
        "label": "Hôtellerie",
        "icon": "🏨",
        "types": ["hotel", "motel", "lodging", "bed_and_breakfast", "resort_hotel"],
    },
    "sport": {
        "label": "Sport & Fitness",
        "icon": "🏋️",
        "types": ["gym", "fitness_center", "sports_club", "sports_complex", "swimming_pool"],
    },
}

# Domaines de messagerie gratuite à exclure de la collecte massive
FREE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com",
    "hotmail.com", "hotmail.fr", "hotmail.be", "hotmail.es",
    "yahoo.com", "yahoo.fr", "yahoo.co.uk", "yahoo.es", "yahoo.be",
    "outlook.com", "outlook.fr", "live.com", "live.fr", "live.be",
    "orange.fr", "wanadoo.fr",
    "free.fr",
    "laposte.net", "laposte.fr",
    "sfr.fr", "sfr.net",
    "bbox.fr",
    "numericable.fr",
    "icloud.com", "me.com", "mac.com",
    "msn.com", "aol.com", "aol.fr",
    "protonmail.com", "protonmail.ch",
    "pm.me",
}

# Colonnes CSV pour la collecte massive (inclut place_id pour la reprise)
CSV_COLUMNS_MASSIVE = [
    "place_id",
    "nom",
    "adresse",
    "telephone",
    "email",
    "site_web",
    "note_google",
    "nombre_avis",
    "statut",
    "types",
    "ville",
    "departement",
    "secteur",
    "type_business",
]

# Départements français (code → nom)
FRENCH_DEPARTMENTS = {
    "01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes", "06": "Alpes-Maritimes", "07": "Ardèche", "08": "Ardennes",
    "09": "Ariège", "10": "Aube", "11": "Aude", "12": "Aveyron",
    "13": "Bouches-du-Rhône", "14": "Calvados", "15": "Cantal", "16": "Charente",
    "17": "Charente-Maritime", "18": "Cher", "19": "Corrèze",
    "2A": "Corse-du-Sud", "2B": "Haute-Corse",
    "21": "Côte-d'Or", "22": "Côtes-d'Armor", "23": "Creuse",
    "24": "Dordogne", "25": "Doubs", "26": "Drôme", "27": "Eure",
    "28": "Eure-et-Loir", "29": "Finistère", "30": "Gard", "31": "Haute-Garonne",
    "32": "Gers", "33": "Gironde", "34": "Hérault", "35": "Ille-et-Vilaine",
    "36": "Indre", "37": "Indre-et-Loire", "38": "Isère", "39": "Jura",
    "40": "Landes", "41": "Loir-et-Cher", "42": "Loire", "43": "Haute-Loire",
    "44": "Loire-Atlantique", "45": "Loiret", "46": "Lot", "47": "Lot-et-Garonne",
    "48": "Lozère", "49": "Maine-et-Loire", "50": "Manche", "51": "Marne",
    "52": "Haute-Marne", "53": "Mayenne", "54": "Meurthe-et-Moselle", "55": "Meuse",
    "56": "Morbihan", "57": "Moselle", "58": "Nièvre", "59": "Nord",
    "60": "Oise", "61": "Orne", "62": "Pas-de-Calais", "63": "Puy-de-Dôme",
    "64": "Pyrénées-Atlantiques", "65": "Hautes-Pyrénées", "66": "Pyrénées-Orientales",
    "67": "Bas-Rhin", "68": "Haut-Rhin", "69": "Rhône", "70": "Haute-Saône",
    "71": "Saône-et-Loire", "72": "Sarthe", "73": "Savoie", "74": "Haute-Savoie",
    "75": "Paris", "76": "Seine-Maritime", "77": "Seine-et-Marne", "78": "Yvelines",
    "79": "Deux-Sèvres", "80": "Somme", "81": "Tarn", "82": "Tarn-et-Garonne",
    "83": "Var", "84": "Vaucluse", "85": "Vendée", "86": "Vienne",
    "87": "Haute-Vienne", "88": "Vosges", "89": "Yonne",
    "90": "Territoire de Belfort", "91": "Essonne", "92": "Hauts-de-Seine",
    "93": "Seine-Saint-Denis", "94": "Val-de-Marne", "95": "Val-d'Oise",
    "971": "Guadeloupe", "972": "Martinique", "973": "Guyane",
    "974": "La Réunion", "976": "Mayotte",
}

# 550+ villes françaises : (nom, code_dept, nom_dept)
FRENCH_CITIES = [
    # ── Paris ─────────────────────────────────────────────────────────────────
    ("Paris", "75", "Paris"),

    # ── Hauts-de-Seine (92) ───────────────────────────────────────────────────
    ("Boulogne-Billancourt", "92", "Hauts-de-Seine"),
    ("Nanterre", "92", "Hauts-de-Seine"),
    ("Colombes", "92", "Hauts-de-Seine"),
    ("Rueil-Malmaison", "92", "Hauts-de-Seine"),
    ("Asnières-sur-Seine", "92", "Hauts-de-Seine"),
    ("Courbevoie", "92", "Hauts-de-Seine"),
    ("Levallois-Perret", "92", "Hauts-de-Seine"),
    ("Issy-les-Moulineaux", "92", "Hauts-de-Seine"),
    ("Clichy", "92", "Hauts-de-Seine"),
    ("Neuilly-sur-Seine", "92", "Hauts-de-Seine"),
    ("Gennevilliers", "92", "Hauts-de-Seine"),
    ("Clamart", "92", "Hauts-de-Seine"),
    ("Villeneuve-la-Garenne", "92", "Hauts-de-Seine"),
    ("Châtenay-Malabry", "92", "Hauts-de-Seine"),
    ("Bagneux", "92", "Hauts-de-Seine"),
    ("Sceaux", "92", "Hauts-de-Seine"),
    ("Antony", "92", "Hauts-de-Seine"),
    ("Vanves", "92", "Hauts-de-Seine"),

    # ── Seine-Saint-Denis (93) ────────────────────────────────────────────────
    ("Saint-Denis", "93", "Seine-Saint-Denis"),
    ("Montreuil", "93", "Seine-Saint-Denis"),
    ("Aubervilliers", "93", "Seine-Saint-Denis"),
    ("Aulnay-sous-Bois", "93", "Seine-Saint-Denis"),
    ("Noisy-le-Grand", "93", "Seine-Saint-Denis"),
    ("Pantin", "93", "Seine-Saint-Denis"),
    ("Drancy", "93", "Seine-Saint-Denis"),
    ("Bobigny", "93", "Seine-Saint-Denis"),
    ("Saint-Ouen-sur-Seine", "93", "Seine-Saint-Denis"),
    ("Bondy", "93", "Seine-Saint-Denis"),
    ("Livry-Gargan", "93", "Seine-Saint-Denis"),
    ("Gagny", "93", "Seine-Saint-Denis"),
    ("Rosny-sous-Bois", "93", "Seine-Saint-Denis"),
    ("La Courneuve", "93", "Seine-Saint-Denis"),
    ("Bagnolet", "93", "Seine-Saint-Denis"),
    ("Épinay-sur-Seine", "93", "Seine-Saint-Denis"),
    ("Tremblay-en-France", "93", "Seine-Saint-Denis"),
    ("Stains", "93", "Seine-Saint-Denis"),

    # ── Val-de-Marne (94) ─────────────────────────────────────────────────────
    ("Créteil", "94", "Val-de-Marne"),
    ("Vitry-sur-Seine", "94", "Val-de-Marne"),
    ("Champigny-sur-Marne", "94", "Val-de-Marne"),
    ("Vincennes", "94", "Val-de-Marne"),
    ("Ivry-sur-Seine", "94", "Val-de-Marne"),
    ("Fontenay-sous-Bois", "94", "Val-de-Marne"),
    ("Saint-Maur-des-Fossés", "94", "Val-de-Marne"),
    ("Maisons-Alfort", "94", "Val-de-Marne"),
    ("Nogent-sur-Marne", "94", "Val-de-Marne"),
    ("Villeneuve-Saint-Georges", "94", "Val-de-Marne"),
    ("Choisy-le-Roi", "94", "Val-de-Marne"),
    ("Charenton-le-Pont", "94", "Val-de-Marne"),

    # ── Essonne (91) ──────────────────────────────────────────────────────────
    ("Évry-Courcouronnes", "91", "Essonne"),
    ("Corbeil-Essonnes", "91", "Essonne"),
    ("Massy", "91", "Essonne"),
    ("Savigny-sur-Orge", "91", "Essonne"),
    ("Palaiseau", "91", "Essonne"),
    ("Viry-Châtillon", "91", "Essonne"),
    ("Athis-Mons", "91", "Essonne"),
    ("Longjumeau", "91", "Essonne"),
    ("Sainte-Geneviève-des-Bois", "91", "Essonne"),

    # ── Yvelines (78) ─────────────────────────────────────────────────────────
    ("Versailles", "78", "Yvelines"),
    ("Mantes-la-Jolie", "78", "Yvelines"),
    ("Poissy", "78", "Yvelines"),
    ("Sartrouville", "78", "Yvelines"),
    ("Conflans-Sainte-Honorine", "78", "Yvelines"),
    ("Rambouillet", "78", "Yvelines"),
    ("Trappes", "78", "Yvelines"),
    ("Guyancourt", "78", "Yvelines"),
    ("Élancourt", "78", "Yvelines"),

    # ── Val-d'Oise (95) ───────────────────────────────────────────────────────
    ("Cergy", "95", "Val-d'Oise"),
    ("Argenteuil", "95", "Val-d'Oise"),
    ("Sarcelles", "95", "Val-d'Oise"),
    ("Pontoise", "95", "Val-d'Oise"),
    ("Gonesse", "95", "Val-d'Oise"),
    ("Garges-lès-Gonesse", "95", "Val-d'Oise"),
    ("Saint-Gratien", "95", "Val-d'Oise"),
    ("Bezons", "95", "Val-d'Oise"),
    ("Ermont", "95", "Val-d'Oise"),

    # ── Seine-et-Marne (77) ───────────────────────────────────────────────────
    ("Meaux", "77", "Seine-et-Marne"),
    ("Melun", "77", "Seine-et-Marne"),
    ("Chelles", "77", "Seine-et-Marne"),
    ("Pontault-Combault", "77", "Seine-et-Marne"),
    ("Savigny-le-Temple", "77", "Seine-et-Marne"),
    ("Torcy", "77", "Seine-et-Marne"),
    ("Moissy-Cramayel", "77", "Seine-et-Marne"),
    ("Combs-la-Ville", "77", "Seine-et-Marne"),

    # ── Nord (59) ─────────────────────────────────────────────────────────────
    ("Lille", "59", "Nord"),
    ("Roubaix", "59", "Nord"),
    ("Tourcoing", "59", "Nord"),
    ("Valenciennes", "59", "Nord"),
    ("Dunkerque", "59", "Nord"),
    ("Douai", "59", "Nord"),
    ("Villeneuve-d'Ascq", "59", "Nord"),
    ("Maubeuge", "59", "Nord"),
    ("Armentières", "59", "Nord"),
    ("Marcq-en-Barœul", "59", "Nord"),
    ("Lambersart", "59", "Nord"),
    ("Wasquehal", "59", "Nord"),
    ("Faches-Thumesnil", "59", "Nord"),
    ("Mons-en-Barœul", "59", "Nord"),
    ("Grande-Synthe", "59", "Nord"),
    ("Wattrelos", "59", "Nord"),
    ("Croix", "59", "Nord"),
    ("Loos", "59", "Nord"),
    ("Cambrai", "59", "Nord"),

    # ── Pas-de-Calais (62) ────────────────────────────────────────────────────
    ("Lens", "62", "Pas-de-Calais"),
    ("Arras", "62", "Pas-de-Calais"),
    ("Calais", "62", "Pas-de-Calais"),
    ("Boulogne-sur-Mer", "62", "Pas-de-Calais"),
    ("Béthune", "62", "Pas-de-Calais"),
    ("Hénin-Beaumont", "62", "Pas-de-Calais"),
    ("Liévin", "62", "Pas-de-Calais"),
    ("Saint-Omer", "62", "Pas-de-Calais"),
    ("Bruay-la-Buissière", "62", "Pas-de-Calais"),
    ("Auchel", "62", "Pas-de-Calais"),

    # ── Somme (80) ────────────────────────────────────────────────────────────
    ("Amiens", "80", "Somme"),
    ("Abbeville", "80", "Somme"),
    ("Albert", "80", "Somme"),

    # ── Oise (60) ─────────────────────────────────────────────────────────────
    ("Beauvais", "60", "Oise"),
    ("Creil", "60", "Oise"),
    ("Compiègne", "60", "Oise"),
    ("Nogent-sur-Oise", "60", "Oise"),
    ("Senlis", "60", "Oise"),

    # ── Aisne (02) ────────────────────────────────────────────────────────────
    ("Saint-Quentin", "02", "Aisne"),
    ("Laon", "02", "Aisne"),
    ("Soissons", "02", "Aisne"),
    ("Château-Thierry", "02", "Aisne"),

    # ── Seine-Maritime (76) ───────────────────────────────────────────────────
    ("Rouen", "76", "Seine-Maritime"),
    ("Le Havre", "76", "Seine-Maritime"),
    ("Dieppe", "76", "Seine-Maritime"),
    ("Sotteville-lès-Rouen", "76", "Seine-Maritime"),
    ("Mont-Saint-Aignan", "76", "Seine-Maritime"),
    ("Maromme", "76", "Seine-Maritime"),
    ("Fécamp", "76", "Seine-Maritime"),
    ("Elbeuf", "76", "Seine-Maritime"),

    # ── Calvados (14) ─────────────────────────────────────────────────────────
    ("Caen", "14", "Calvados"),
    ("Hérouville-Saint-Clair", "14", "Calvados"),
    ("Lisieux", "14", "Calvados"),
    ("Bayeux", "14", "Calvados"),

    # ── Eure (27) ─────────────────────────────────────────────────────────────
    ("Évreux", "27", "Eure"),
    ("Vernon", "27", "Eure"),
    ("Louviers", "27", "Eure"),

    # ── Manche (50) ───────────────────────────────────────────────────────────
    ("Cherbourg-en-Cotentin", "50", "Manche"),
    ("Saint-Lô", "50", "Manche"),
    ("Avranches", "50", "Manche"),
    ("Granville", "50", "Manche"),

    # ── Orne (61) ─────────────────────────────────────────────────────────────
    ("Alençon", "61", "Orne"),
    ("Flers", "61", "Orne"),
    ("Argentan", "61", "Orne"),

    # ── Ille-et-Vilaine (35) ──────────────────────────────────────────────────
    ("Rennes", "35", "Ille-et-Vilaine"),
    ("Saint-Malo", "35", "Ille-et-Vilaine"),
    ("Bruz", "35", "Ille-et-Vilaine"),
    ("Fougères", "35", "Ille-et-Vilaine"),
    ("Vitré", "35", "Ille-et-Vilaine"),
    ("Cesson-Sévigné", "35", "Ille-et-Vilaine"),

    # ── Finistère (29) ────────────────────────────────────────────────────────
    ("Brest", "29", "Finistère"),
    ("Quimper", "29", "Finistère"),
    ("Concarneau", "29", "Finistère"),
    ("Morlaix", "29", "Finistère"),
    ("Quimperlé", "29", "Finistère"),
    ("Landerneau", "29", "Finistère"),

    # ── Côtes-d'Armor (22) ────────────────────────────────────────────────────
    ("Saint-Brieuc", "22", "Côtes-d'Armor"),
    ("Lannion", "22", "Côtes-d'Armor"),
    ("Dinan", "22", "Côtes-d'Armor"),
    ("Loudéac", "22", "Côtes-d'Armor"),

    # ── Morbihan (56) ─────────────────────────────────────────────────────────
    ("Lorient", "56", "Morbihan"),
    ("Vannes", "56", "Morbihan"),
    ("Pontivy", "56", "Morbihan"),
    ("Lanester", "56", "Morbihan"),

    # ── Loire-Atlantique (44) ─────────────────────────────────────────────────
    ("Nantes", "44", "Loire-Atlantique"),
    ("Saint-Nazaire", "44", "Loire-Atlantique"),
    ("Saint-Herblain", "44", "Loire-Atlantique"),
    ("Rezé", "44", "Loire-Atlantique"),
    ("Orvault", "44", "Loire-Atlantique"),
    ("La Baule-Escoublac", "44", "Loire-Atlantique"),
    ("Carquefou", "44", "Loire-Atlantique"),
    ("Vertou", "44", "Loire-Atlantique"),

    # ── Maine-et-Loire (49) ───────────────────────────────────────────────────
    ("Angers", "49", "Maine-et-Loire"),
    ("Cholet", "49", "Maine-et-Loire"),
    ("Saumur", "49", "Maine-et-Loire"),
    ("Avrillé", "49", "Maine-et-Loire"),
    ("Trélazé", "49", "Maine-et-Loire"),

    # ── Sarthe (72) ───────────────────────────────────────────────────────────
    ("Le Mans", "72", "Sarthe"),
    ("La Flèche", "72", "Sarthe"),
    ("Allonnes", "72", "Sarthe"),

    # ── Vendée (85) ───────────────────────────────────────────────────────────
    ("La Roche-sur-Yon", "85", "Vendée"),
    ("Les Sables-d'Olonne", "85", "Vendée"),
    ("Fontenay-le-Comte", "85", "Vendée"),
    ("Saint-Jean-de-Monts", "85", "Vendée"),
    ("Les Herbiers", "85", "Vendée"),

    # ── Mayenne (53) ──────────────────────────────────────────────────────────
    ("Laval", "53", "Mayenne"),
    ("Mayenne", "53", "Mayenne"),
    ("Château-Gontier", "53", "Mayenne"),

    # ── Indre-et-Loire (37) ───────────────────────────────────────────────────
    ("Tours", "37", "Indre-et-Loire"),
    ("Joué-lès-Tours", "37", "Indre-et-Loire"),
    ("Saint-Pierre-des-Corps", "37", "Indre-et-Loire"),
    ("Amboise", "37", "Indre-et-Loire"),
    ("Chinon", "37", "Indre-et-Loire"),

    # ── Loiret (45) ───────────────────────────────────────────────────────────
    ("Orléans", "45", "Loiret"),
    ("Fleury-les-Aubrais", "45", "Loiret"),
    ("Saint-Jean-de-Braye", "45", "Loiret"),
    ("Montargis", "45", "Loiret"),
    ("Olivet", "45", "Loiret"),

    # ── Eure-et-Loir (28) ────────────────────────────────────────────────────
    ("Chartres", "28", "Eure-et-Loir"),
    ("Dreux", "28", "Eure-et-Loir"),
    ("Châteaudun", "28", "Eure-et-Loir"),

    # ── Loir-et-Cher (41) ────────────────────────────────────────────────────
    ("Blois", "41", "Loir-et-Cher"),
    ("Vendôme", "41", "Loir-et-Cher"),
    ("Romorantin-Lanthenay", "41", "Loir-et-Cher"),

    # ── Cher (18) ─────────────────────────────────────────────────────────────
    ("Bourges", "18", "Cher"),
    ("Saint-Amand-Montrond", "18", "Cher"),
    ("Vierzon", "18", "Cher"),

    # ── Indre (36) ────────────────────────────────────────────────────────────
    ("Châteauroux", "36", "Indre"),
    ("Issoudun", "36", "Indre"),

    # ── Marne (51) ────────────────────────────────────────────────────────────
    ("Reims", "51", "Marne"),
    ("Châlons-en-Champagne", "51", "Marne"),
    ("Épernay", "51", "Marne"),
    ("Vitry-le-François", "51", "Marne"),

    # ── Aube (10) ─────────────────────────────────────────────────────────────
    ("Troyes", "10", "Aube"),
    ("Romilly-sur-Seine", "10", "Aube"),

    # ── Ardennes (08) ────────────────────────────────────────────────────────
    ("Charleville-Mézières", "08", "Ardennes"),
    ("Sedan", "08", "Ardennes"),

    # ── Haute-Marne (52) ─────────────────────────────────────────────────────
    ("Chaumont", "52", "Haute-Marne"),
    ("Saint-Dizier", "52", "Haute-Marne"),

    # ── Bas-Rhin (67) ─────────────────────────────────────────────────────────
    ("Strasbourg", "67", "Bas-Rhin"),
    ("Haguenau", "67", "Bas-Rhin"),
    ("Schiltigheim", "67", "Bas-Rhin"),
    ("Illkirch-Graffenstaden", "67", "Bas-Rhin"),
    ("Sélestat", "67", "Bas-Rhin"),
    ("Obernai", "67", "Bas-Rhin"),
    ("Saverne", "67", "Bas-Rhin"),
    ("Lingolsheim", "67", "Bas-Rhin"),

    # ── Haut-Rhin (68) ────────────────────────────────────────────────────────
    ("Mulhouse", "68", "Haut-Rhin"),
    ("Colmar", "68", "Haut-Rhin"),
    ("Saint-Louis", "68", "Haut-Rhin"),
    ("Illzach", "68", "Haut-Rhin"),
    ("Rixheim", "68", "Haut-Rhin"),
    ("Wittenheim", "68", "Haut-Rhin"),

    # ── Moselle (57) ──────────────────────────────────────────────────────────
    ("Metz", "57", "Moselle"),
    ("Thionville", "57", "Moselle"),
    ("Forbach", "57", "Moselle"),
    ("Sarrebourg", "57", "Moselle"),
    ("Sarreguemines", "57", "Moselle"),
    ("Hayange", "57", "Moselle"),
    ("Yutz", "57", "Moselle"),
    ("Fameck", "57", "Moselle"),

    # ── Meurthe-et-Moselle (54) ───────────────────────────────────────────────
    ("Nancy", "54", "Meurthe-et-Moselle"),
    ("Vandœuvre-lès-Nancy", "54", "Meurthe-et-Moselle"),
    ("Longwy", "54", "Meurthe-et-Moselle"),
    ("Lunéville", "54", "Meurthe-et-Moselle"),
    ("Briey", "54", "Meurthe-et-Moselle"),
    ("Tomblaine", "54", "Meurthe-et-Moselle"),

    # ── Meuse (55) ────────────────────────────────────────────────────────────
    ("Bar-le-Duc", "55", "Meuse"),
    ("Verdun", "55", "Meuse"),

    # ── Vosges (88) ───────────────────────────────────────────────────────────
    ("Épinal", "88", "Vosges"),
    ("Saint-Dié-des-Vosges", "88", "Vosges"),
    ("Remiremont", "88", "Vosges"),

    # ── Côte-d'Or (21) ────────────────────────────────────────────────────────
    ("Dijon", "21", "Côte-d'Or"),
    ("Beaune", "21", "Côte-d'Or"),
    ("Chenôve", "21", "Côte-d'Or"),
    ("Quetigny", "21", "Côte-d'Or"),

    # ── Saône-et-Loire (71) ───────────────────────────────────────────────────
    ("Chalon-sur-Saône", "71", "Saône-et-Loire"),
    ("Mâcon", "71", "Saône-et-Loire"),
    ("Le Creusot", "71", "Saône-et-Loire"),
    ("Montceau-les-Mines", "71", "Saône-et-Loire"),

    # ── Doubs (25) ────────────────────────────────────────────────────────────
    ("Besançon", "25", "Doubs"),
    ("Montbéliard", "25", "Doubs"),
    ("Pontarlier", "25", "Doubs"),
    ("Sochaux", "25", "Doubs"),
    ("Audincourt", "25", "Doubs"),

    # ── Territoire de Belfort (90) ────────────────────────────────────────────
    ("Belfort", "90", "Territoire de Belfort"),

    # ── Jura (39) ─────────────────────────────────────────────────────────────
    ("Lons-le-Saunier", "39", "Jura"),
    ("Dole", "39", "Jura"),

    # ── Haute-Saône (70) ──────────────────────────────────────────────────────
    ("Vesoul", "70", "Haute-Saône"),
    ("Lure", "70", "Haute-Saône"),

    # ── Yonne (89) ────────────────────────────────────────────────────────────
    ("Auxerre", "89", "Yonne"),
    ("Sens", "89", "Yonne"),
    ("Joigny", "89", "Yonne"),

    # ── Nièvre (58) ───────────────────────────────────────────────────────────
    ("Nevers", "58", "Nièvre"),
    ("Cosne-Cours-sur-Loire", "58", "Nièvre"),

    # ── Rhône (69) ────────────────────────────────────────────────────────────
    ("Lyon", "69", "Rhône"),
    ("Villeurbanne", "69", "Rhône"),
    ("Vénissieux", "69", "Rhône"),
    ("Vaulx-en-Velin", "69", "Rhône"),
    ("Bron", "69", "Rhône"),
    ("Caluire-et-Cuire", "69", "Rhône"),
    ("Saint-Priest", "69", "Rhône"),
    ("Décines-Charpieu", "69", "Rhône"),
    ("Meyzieu", "69", "Rhône"),
    ("Rillieux-la-Pape", "69", "Rhône"),
    ("Oullins", "69", "Rhône"),
    ("Tassin-la-Demi-Lune", "69", "Rhône"),
    ("Mions", "69", "Rhône"),
    ("Givors", "69", "Rhône"),

    # ── Isère (38) ────────────────────────────────────────────────────────────
    ("Grenoble", "38", "Isère"),
    ("Échirolles", "38", "Isère"),
    ("Saint-Martin-d'Hères", "38", "Isère"),
    ("Vienne", "38", "Isère"),
    ("Bourgoin-Jallieu", "38", "Isère"),
    ("Crolles", "38", "Isère"),
    ("Meylan", "38", "Isère"),
    ("Fontaine", "38", "Isère"),

    # ── Loire (42) ────────────────────────────────────────────────────────────
    ("Saint-Étienne", "42", "Loire"),
    ("Roanne", "42", "Loire"),
    ("Firminy", "42", "Loire"),
    ("Rive-de-Gier", "42", "Loire"),
    ("Andrézieux-Bouthéon", "42", "Loire"),
    ("Saint-Chamond", "42", "Loire"),

    # ── Puy-de-Dôme (63) ─────────────────────────────────────────────────────
    ("Clermont-Ferrand", "63", "Puy-de-Dôme"),
    ("Aubière", "63", "Puy-de-Dôme"),
    ("Riom", "63", "Puy-de-Dôme"),
    ("Issoire", "63", "Puy-de-Dôme"),
    ("Thiers", "63", "Puy-de-Dôme"),
    ("Cournon-d'Auvergne", "63", "Puy-de-Dôme"),

    # ── Allier (03) ───────────────────────────────────────────────────────────
    ("Moulins", "03", "Allier"),
    ("Vichy", "03", "Allier"),
    ("Montluçon", "03", "Allier"),

    # ── Haute-Loire (43) ─────────────────────────────────────────────────────
    ("Le Puy-en-Velay", "43", "Haute-Loire"),
    ("Brioude", "43", "Haute-Loire"),

    # ── Cantal (15) ───────────────────────────────────────────────────────────
    ("Aurillac", "15", "Cantal"),

    # ── Savoie (73) ───────────────────────────────────────────────────────────
    ("Chambéry", "73", "Savoie"),
    ("Aix-les-Bains", "73", "Savoie"),
    ("Albertville", "73", "Savoie"),
    ("Bourg-Saint-Maurice", "73", "Savoie"),

    # ── Haute-Savoie (74) ─────────────────────────────────────────────────────
    ("Annecy", "74", "Haute-Savoie"),
    ("Annemasse", "74", "Haute-Savoie"),
    ("Cluses", "74", "Haute-Savoie"),
    ("Thonon-les-Bains", "74", "Haute-Savoie"),
    ("Sallanches", "74", "Haute-Savoie"),
    ("Cran-Gevrier", "74", "Haute-Savoie"),
    ("Seynod", "74", "Haute-Savoie"),

    # ── Ain (01) ──────────────────────────────────────────────────────────────
    ("Bourg-en-Bresse", "01", "Ain"),
    ("Oyonnax", "01", "Ain"),
    ("Ambérieu-en-Bugey", "01", "Ain"),
    ("Bellegarde-sur-Valserine", "01", "Ain"),

    # ── Drôme (26) ────────────────────────────────────────────────────────────
    ("Valence", "26", "Drôme"),
    ("Romans-sur-Isère", "26", "Drôme"),
    ("Montélimar", "26", "Drôme"),
    ("Bourg-lès-Valence", "26", "Drôme"),

    # ── Ardèche (07) ──────────────────────────────────────────────────────────
    ("Privas", "07", "Ardèche"),
    ("Aubenas", "07", "Ardèche"),
    ("Annonay", "07", "Ardèche"),

    # ── Bouches-du-Rhône (13) ─────────────────────────────────────────────────
    ("Marseille", "13", "Bouches-du-Rhône"),
    ("Aix-en-Provence", "13", "Bouches-du-Rhône"),
    ("Arles", "13", "Bouches-du-Rhône"),
    ("Istres", "13", "Bouches-du-Rhône"),
    ("Martigues", "13", "Bouches-du-Rhône"),
    ("Salon-de-Provence", "13", "Bouches-du-Rhône"),
    ("Aubagne", "13", "Bouches-du-Rhône"),
    ("La Ciotat", "13", "Bouches-du-Rhône"),
    ("Vitrolles", "13", "Bouches-du-Rhône"),
    ("Marignane", "13", "Bouches-du-Rhône"),
    ("Miramas", "13", "Bouches-du-Rhône"),
    ("Gardanne", "13", "Bouches-du-Rhône"),

    # ── Var (83) ──────────────────────────────────────────────────────────────
    ("Toulon", "83", "Var"),
    ("La Seyne-sur-Mer", "83", "Var"),
    ("Hyères", "83", "Var"),
    ("Draguignan", "83", "Var"),
    ("Fréjus", "83", "Var"),
    ("La Garde", "83", "Var"),
    ("Six-Fours-les-Plages", "83", "Var"),
    ("Brignoles", "83", "Var"),
    ("Sanary-sur-Mer", "83", "Var"),
    ("Saint-Raphaël", "83", "Var"),

    # ── Alpes-Maritimes (06) ──────────────────────────────────────────────────
    ("Nice", "06", "Alpes-Maritimes"),
    ("Cannes", "06", "Alpes-Maritimes"),
    ("Antibes", "06", "Alpes-Maritimes"),
    ("Grasse", "06", "Alpes-Maritimes"),
    ("Menton", "06", "Alpes-Maritimes"),
    ("Cagnes-sur-Mer", "06", "Alpes-Maritimes"),
    ("Vallauris", "06", "Alpes-Maritimes"),
    ("Vence", "06", "Alpes-Maritimes"),
    ("Mougins", "06", "Alpes-Maritimes"),
    ("Mandelieu-la-Napoule", "06", "Alpes-Maritimes"),

    # ── Vaucluse (84) ─────────────────────────────────────────────────────────
    ("Avignon", "84", "Vaucluse"),
    ("Orange", "84", "Vaucluse"),
    ("Carpentras", "84", "Vaucluse"),
    ("Apt", "84", "Vaucluse"),
    ("Cavaillon", "84", "Vaucluse"),
    ("Le Pontet", "84", "Vaucluse"),

    # ── Alpes-de-Haute-Provence (04) ──────────────────────────────────────────
    ("Digne-les-Bains", "04", "Alpes-de-Haute-Provence"),
    ("Manosque", "04", "Alpes-de-Haute-Provence"),

    # ── Hautes-Alpes (05) ─────────────────────────────────────────────────────
    ("Gap", "05", "Hautes-Alpes"),
    ("Briançon", "05", "Hautes-Alpes"),

    # ── Haute-Garonne (31) ────────────────────────────────────────────────────
    ("Toulouse", "31", "Haute-Garonne"),
    ("Blagnac", "31", "Haute-Garonne"),
    ("Colomiers", "31", "Haute-Garonne"),
    ("Tournefeuille", "31", "Haute-Garonne"),
    ("Muret", "31", "Haute-Garonne"),
    ("Ramonville-Saint-Agne", "31", "Haute-Garonne"),
    ("Cugnaux", "31", "Haute-Garonne"),
    ("Balma", "31", "Haute-Garonne"),
    ("L'Union", "31", "Haute-Garonne"),

    # ── Hérault (34) ──────────────────────────────────────────────────────────
    ("Montpellier", "34", "Hérault"),
    ("Béziers", "34", "Hérault"),
    ("Sète", "34", "Hérault"),
    ("Agde", "34", "Hérault"),
    ("Frontignan", "34", "Hérault"),
    ("Lunel", "34", "Hérault"),
    ("Mauguio", "34", "Hérault"),
    ("Castelnau-le-Lez", "34", "Hérault"),
    ("Lattes", "34", "Hérault"),

    # ── Gard (30) ─────────────────────────────────────────────────────────────
    ("Nîmes", "30", "Gard"),
    ("Alès", "30", "Gard"),
    ("Bagnols-sur-Cèze", "30", "Gard"),
    ("Beaucaire", "30", "Gard"),
    ("Pont-Saint-Esprit", "30", "Gard"),

    # ── Aude (11) ─────────────────────────────────────────────────────────────
    ("Carcassonne", "11", "Aude"),
    ("Narbonne", "11", "Aude"),
    ("Limoux", "11", "Aude"),
    ("Castelnaudary", "11", "Aude"),

    # ── Pyrénées-Orientales (66) ──────────────────────────────────────────────
    ("Perpignan", "66", "Pyrénées-Orientales"),
    ("Canet-en-Roussillon", "66", "Pyrénées-Orientales"),
    ("Rivesaltes", "66", "Pyrénées-Orientales"),
    ("Saint-Estève", "66", "Pyrénées-Orientales"),

    # ── Ariège (09) ───────────────────────────────────────────────────────────
    ("Foix", "09", "Ariège"),
    ("Pamiers", "09", "Ariège"),

    # ── Tarn (81) ─────────────────────────────────────────────────────────────
    ("Albi", "81", "Tarn"),
    ("Castres", "81", "Tarn"),
    ("Mazamet", "81", "Tarn"),
    ("Gaillac", "81", "Tarn"),

    # ── Tarn-et-Garonne (82) ──────────────────────────────────────────────────
    ("Montauban", "82", "Tarn-et-Garonne"),
    ("Castelsarrasin", "82", "Tarn-et-Garonne"),

    # ── Aveyron (12) ──────────────────────────────────────────────────────────
    ("Rodez", "12", "Aveyron"),
    ("Millau", "12", "Aveyron"),
    ("Villefranche-de-Rouergue", "12", "Aveyron"),

    # ── Lot (46) ──────────────────────────────────────────────────────────────
    ("Cahors", "46", "Lot"),
    ("Figeac", "46", "Lot"),

    # ── Gers (32) ─────────────────────────────────────────────────────────────
    ("Auch", "32", "Gers"),

    # ── Hautes-Pyrénées (65) ──────────────────────────────────────────────────
    ("Tarbes", "65", "Hautes-Pyrénées"),
    ("Lourdes", "65", "Hautes-Pyrénées"),

    # ── Pyrénées-Atlantiques (64) ─────────────────────────────────────────────
    ("Pau", "64", "Pyrénées-Atlantiques"),
    ("Bayonne", "64", "Pyrénées-Atlantiques"),
    ("Biarritz", "64", "Pyrénées-Atlantiques"),
    ("Anglet", "64", "Pyrénées-Atlantiques"),
    ("Hendaye", "64", "Pyrénées-Atlantiques"),
    ("Orthez", "64", "Pyrénées-Atlantiques"),

    # ── Lozère (48) ───────────────────────────────────────────────────────────
    ("Mende", "48", "Lozère"),

    # ── Gironde (33) ──────────────────────────────────────────────────────────
    ("Bordeaux", "33", "Gironde"),
    ("Mérignac", "33", "Gironde"),
    ("Pessac", "33", "Gironde"),
    ("Talence", "33", "Gironde"),
    ("Gradignan", "33", "Gironde"),
    ("Lormont", "33", "Gironde"),
    ("Libourne", "33", "Gironde"),
    ("Arcachon", "33", "Gironde"),
    ("Gujan-Mestras", "33", "Gironde"),
    ("Begles", "33", "Gironde"),
    ("Villenave-d'Ornon", "33", "Gironde"),
    ("Langon", "33", "Gironde"),

    # ── Charente (16) ─────────────────────────────────────────────────────────
    ("Angoulême", "16", "Charente"),
    ("Cognac", "16", "Charente"),
    ("Soyaux", "16", "Charente"),

    # ── Charente-Maritime (17) ────────────────────────────────────────────────
    ("La Rochelle", "17", "Charente-Maritime"),
    ("Rochefort", "17", "Charente-Maritime"),
    ("Saintes", "17", "Charente-Maritime"),
    ("Royan", "17", "Charente-Maritime"),
    ("Châtelaillon-Plage", "17", "Charente-Maritime"),

    # ── Dordogne (24) ─────────────────────────────────────────────────────────
    ("Périgueux", "24", "Dordogne"),
    ("Bergerac", "24", "Dordogne"),
    ("Sarlat-la-Canéda", "24", "Dordogne"),

    # ── Lot-et-Garonne (47) ───────────────────────────────────────────────────
    ("Agen", "47", "Lot-et-Garonne"),
    ("Villeneuve-sur-Lot", "47", "Lot-et-Garonne"),
    ("Marmande", "47", "Lot-et-Garonne"),

    # ── Landes (40) ───────────────────────────────────────────────────────────
    ("Mont-de-Marsan", "40", "Landes"),
    ("Dax", "40", "Landes"),
    ("Biscarrosse", "40", "Landes"),

    # ── Corrèze (19) ──────────────────────────────────────────────────────────
    ("Brive-la-Gaillarde", "19", "Corrèze"),
    ("Tulle", "19", "Corrèze"),
    ("Ussel", "19", "Corrèze"),

    # ── Haute-Vienne (87) ─────────────────────────────────────────────────────
    ("Limoges", "87", "Haute-Vienne"),
    ("Couzeix", "87", "Haute-Vienne"),

    # ── Creuse (23) ───────────────────────────────────────────────────────────
    ("Guéret", "23", "Creuse"),

    # ── Vienne (86) ───────────────────────────────────────────────────────────
    ("Poitiers", "86", "Vienne"),
    ("Châtellerault", "86", "Vienne"),
    ("Buxerolles", "86", "Vienne"),

    # ── Deux-Sèvres (79) ──────────────────────────────────────────────────────
    ("Niort", "79", "Deux-Sèvres"),
    ("Bressuire", "79", "Deux-Sèvres"),

    # ── Vosges déjà traité ci-dessus ──────────────────────────────────────────

    # ── Corse ─────────────────────────────────────────────────────────────────
    ("Ajaccio", "2A", "Corse-du-Sud"),
    ("Porto-Vecchio", "2A", "Corse-du-Sud"),
    ("Bastia", "2B", "Haute-Corse"),
    ("Corte", "2B", "Haute-Corse"),

    # ── DOM-TOM ───────────────────────────────────────────────────────────────
    ("Pointe-à-Pitre", "971", "Guadeloupe"),
    ("Les Abymes", "971", "Guadeloupe"),
    ("Baie-Mahault", "971", "Guadeloupe"),
    ("Fort-de-France", "972", "Martinique"),
    ("Le Lamentin", "972", "Martinique"),
    ("Le Robert", "972", "Martinique"),
    ("Cayenne", "973", "Guyane"),
    ("Saint-Laurent-du-Maroni", "973", "Guyane"),
    ("Saint-Denis de la Réunion", "974", "La Réunion"),
    ("Saint-Paul de la Réunion", "974", "La Réunion"),
    ("Saint-Pierre de la Réunion", "974", "La Réunion"),
    ("Le Tampon", "974", "La Réunion"),
]

# ── Scraping d'emails ─────────────────────────────────────────────────────────

# Regex standard pour détecter les adresses email dans du HTML brut
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Regex emails en contexte HTML : >email@domain< (texte entre balises, y compris dans scripts SSR)
_HTML_CONTEXT_EMAIL_RE = re.compile(
    r">[\s\u00a0]*([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})[\s\u00a0]*<"
)

# Parties locales à rejeter définitivement (emails système/registrar, jamais un contact réel)
_LOCAL_REJECT = frozenset({
    "abuse", "hostmaster", "postmaster", "support", "sales",
    "tldsupport", "noreply", "no-reply", "donotreply", "do-not-reply",
    "mailer-daemon", "bounce", "webmaster", "juridique",
    "dpo", "rgpd", "gdpr", "privacy", "legal", "compliance",
    "spam", "phishing", "report", "security", "helpdesk",
})

# Préfixes génériques à déprioritiser (valides mais moins bons)
_GENERIC_PREFIXES = {
    "contact", "info", "hello", "bonjour", "accueil",
    "administration", "admin", "secretariat",
}

# Domaines techniques/système à exclure totalement (substring matching)
_DOMAIN_BLACKLIST = {
    # Erreurs / placeholders (anglais et français)
    "sentry.io", "example.com", "example.org", "example.net",
    "exemple.com", "exemple.fr", "exemple.org",
    "test.com", "test.fr", "domain.com", "yourdomain.com", "email.com",
    "votre-domaine.fr", "mondomaine.fr", "votresite.fr", "monsite.fr",
    "votre-email.com", "tonemail.com",
    # Hébergement / CDN
    "amazonaws.com", "cloudflare.com", "cloudfront.net",
    "fastly.net", "akamaiedge.net", "akamai.net",
    "gstatic", "googleapis.com", "googletagmanager.com",
    "google.com", "google.fr", "googlemail.com",
    # Plateformes CMS / e-commerce
    "wixpress.com", "wixstatic", "wix.com", "wixsite.com",
    "squarespace.com", "squarespace-cdn.com",
    "shopify.com", "myshopify.com",
    "wordpress.com", "wp.com",
    "jimdo.com", "weebly.com", "webflow.io",
    "prestashop.com", "magento.com",
    # Réseaux sociaux
    "facebook.com", "fbcdn.net", "instagram.com",
    "twitter.com", "t.co", "linkedin.com",
    "tiktok.com", "snapchat.com", "youtube.com",
    # Email marketing / transactionnel
    "mailchimp.com", "mailjet.com", "sendinblue.com", "brevo.com",
    "mailgun.org", "sendgrid.net", "mandrillapp.com",
    "constantcontact.com", "klaviyo.com",
    # Analytics / tracking
    "doubleclick.net", "googlesyndication.com",
    "pixel.facebook.com", "analytics.google.com",
    "hotjar.com", "segment.io", "mixpanel.com",
    "intercom.io", "intercom.com",
    # Schemas / ontologies
    "w3.org", "schema.org", "ogp.me",
    # Registrars / bureaux d'enregistrement (emails WHOIS/admin, jamais propriétaire)
    "registrarsafe", "key-systems", "godaddy", "1und1",
    "openprovider", "cscglobal", "gandi.net",
    "infomaniak.com", "ionos.com", "ionos.de",
    # CDN / ressources statiques
    "bootstrapcdn", "cloudinary.com", "unpkg.com", "jsdelivr.net",
    # Divers plateformes/services parasites
    "axept", "buymeacoffee", "resto-pro",
    "creativecommons", "gravatar.com",
    # Artefacts regex fréquents
    "2x.png", "3x.png", "1x.png",
}

# Extensions qui ne sont jamais des emails (artefacts regex)
_FAKE_TLDS = {
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
    ".js", ".ts", ".jsx", ".tsx", ".mjs",
    ".css", ".scss", ".less",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
    ".map", ".json", ".xml", ".yaml", ".yml",
    ".min", ".gz", ".zip",
}

# Mots JS/CSS interdits dans la partie locale d'un email
# Vérifié segment par segment (après split sur ".")
_JS_CSS_LOCAL_BLACKLIST = frozenset({
    # Objets / variables JS globaux
    "window", "document", "navigator", "location", "history",
    "screen", "console", "process", "global", "globalthis",
    "module", "exports", "require", "prototype", "constructor",
    "arguments", "undefined", "null", "nan", "infinity",
    # Mots-clés JS
    "function", "return", "typeof", "instanceof", "async", "await",
    "var", "let", "const", "this", "self", "super",
    "class", "extends", "import", "export", "default",
    "if", "else", "for", "while", "switch", "case",
    "break", "continue", "try", "catch", "throw", "new", "delete",
    "true", "false", "void",
    # Méthodes Math / utilitaires
    "floor", "ceil", "round", "max", "min", "random", "sqrt", "abs",
    "parse", "stringify", "encode", "decode", "escape",
    # CSS / layout
    "block", "inline", "flex", "grid", "none", "auto",
    "header", "footer", "sidebar", "wrapper", "container",
    "overlay", "modal", "static", "template", "component",
    # Mots techniques divers
    "body", "head", "html", "href", "src",
    "style", "class", "type", "value", "name",
})

# Pages de contact courantes à sonder si la page principale est vide
_CONTACT_PATHS = [
    # Contact direct
    "/contact", "/contact/", "/contact.html", "/contact.php",
    "/contact-us", "/contactus", "/nous-contacter", "/contactez-nous",
    # Mentions légales (obligatoires en France → email souvent présent)
    "/mentions-legales", "/mentions_legales", "/mentions-legales.html",
    "/mentions-legales.php", "/mentions-légales", "/ml", "/legal",
    # À propos / équipe
    "/a-propos", "/a-propos.html", "/about", "/about-us", "/about.html",
    "/qui-sommes-nous", "/equipe", "/team",
    # Équipe & direction (ajouts)
    "/staff", "/direction", "/gerant", "/proprietaire", "/chef",
    "/presentation", "/qui-nous-sommes", "/notre-equipe", "/la-team",
    "/responsable",
    # Pages spécifiques aux secteurs visés
    "/reservation", "/reservations", "/book", "/booking",
    "/informations", "/coordonnees", "/coordonnées", "/infos",
    "/footer", "/sitemap",
]

# Regex emails obfusqués : "nom [at] domaine [dot] fr" ou variantes
_OBFUSCATED_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+"
    r"\s*[\[\(\{]?\s*(?:at|@)\s*[\]\)\}]?\s*"
    r"[a-zA-Z0-9.\-]+"
    r"\s*[\[\(\{]?\s*(?:dot|\.)\s*[\]\)\}]?\s*"
    r"[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Regex pour détecter un lien Facebook dans le HTML
_FB_URL_RE = re.compile(
    r'https?://(?:www\.)?facebook\.com/(?!sharer|share|dialog|tr\b)[^"\s\'<>&?]{3,}',
)

# En-tête HTTP pour éviter les blocages basiques (User-Agent navigateur)
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}


# ── Helpers email scraping ────────────────────────────────────────────────────

def _decode_cloudflare_email(encoded: str) -> str:
    """
    Décode un email protégé par Cloudflare (attribut data-cfemail).
    Algorithme : XOR de chaque octet avec le premier octet (clé).
    """
    try:
        data = bytes.fromhex(encoded)
        key = data[0]
        return "".join(chr(b ^ key) for b in data[1:])
    except Exception:
        return ""


def _deobfuscate_email(text: str) -> str:
    """
    Reconstitue un email obfusqué du type "nom [at] domaine [dot] fr".
    Gère aussi les variantes (at), {at}, espaces parasites, etc.
    """
    text = re.sub(r'\s*[\[\(\{]?\s*at\s*[\]\)\}]?\s*', '@', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*[\[\(\{]?\s*dot\s*[\]\)\}]?\s*', '.', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', '', text)
    return text.lower()


def _extract_jsonld_emails(data: object, found: set) -> None:
    """Parcourt récursivement un objet JSON-LD et extrait les emails."""
    if isinstance(data, dict):
        for key, val in data.items():
            if key.lower() in ("email", "e-mail") and isinstance(val, str):
                if _is_valid_email(val.lower()):
                    found.add(val.lower())
            elif isinstance(val, (dict, list)):
                _extract_jsonld_emails(val, found)
    elif isinstance(data, list):
        for item in data:
            _extract_jsonld_emails(item, found)


def _extract_emails_from_page(soup: BeautifulSoup, html: str = "") -> set:
    """
    Extrait les emails depuis 4 sources :
      1. <a href="mailto:..."> + décodage Cloudflare data-cfemail
      2. JSON-LD <script type="application/ld+json">
      3. Attributs content des balises <meta>
      4. Emails en contexte HTML (>email@domain<) — capte les pages SSR/JSX
    """
    found: set = set()

    # ── 1. <a href="mailto:"> ─────────────────────────────────────────────────
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.lower().startswith("mailto:"):
            email = href[7:].split("?")[0].strip().lower()
            if _is_valid_email(email):
                found.add(email)

    # ── 1b. Cloudflare data-cfemail (encodage de liens mailto) ────────────────
    for tag in soup.find_all(attrs={"data-cfemail": True}):
        decoded = _decode_cloudflare_email(tag["data-cfemail"])
        if decoded and _is_valid_email(decoded):
            found.add(decoded.lower())

    # ── 2. JSON-LD ────────────────────────────────────────────────────────────
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            _extract_jsonld_emails(data, found)
        except Exception:
            pass

    # ── 3. <meta> tags ────────────────────────────────────────────────────────
    for meta in soup.find_all("meta"):
        content = meta.get("content", "")
        if "@" not in content:
            continue
        for email in _EMAIL_RE.findall(content):
            if _is_valid_email(email.lower()):
                found.add(email.lower())

    # ── 4. Contexte HTML >email@domain< (pages SSR, JSX sérialisé) ───────────
    if html and "@" in html:
        for email in _HTML_CONTEXT_EMAIL_RE.findall(html):
            if _is_valid_email(email.lower()):
                found.add(email.lower())

    return found


def _extract_emails_from_visible_text(soup: BeautifulSoup) -> set:
    """
    Fallback : regex sur le texte visible uniquement (hors <script> et <style>).
    Évite les faux positifs JS/CSS en ne travaillant que sur le contenu affiché.
    Modifie le soup en place (decompose) — ne pas réutiliser après appel.
    """
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    found: set = set()
    for email in _EMAIL_RE.findall(text):
        if _is_valid_email(email.lower()):
            found.add(email.lower())
    return found


# Alias de compatibilité pour _playwright_scrape et _scrape_facebook_email
def _extract_emails_from_soup(soup: BeautifulSoup, html: str, found: set) -> None:
    found.update(_extract_emails_from_page(soup, html))


def _scrape_facebook_email(fb_url: str, timeout: int = 8) -> str:
    """
    Tente d'extraire un email professionnel depuis une page Facebook.
    Meta peut bloquer : encapsulé dans un try/except robuste.
    """
    try:
        resp = requests.get(
            fb_url,
            headers=_BROWSER_HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        found: set = set()
        _extract_emails_from_soup(soup, resp.text, found)
        professional = [e for e in found if _is_professional_email(e)]
        if professional:
            return sorted(professional, key=_email_priority)[0]
    except Exception:
        pass
    return ""


def _whois_email(domain: str) -> str:
    """
    Tente d'extraire l'email du registrant via WHOIS (python-whois).
    Fonctionnel uniquement si python-whois est installé.
    """
    if not _WHOIS_OK:
        return ""
    try:
        w = _whois_lib.whois(domain)
        emails = w.emails or []
        if isinstance(emails, str):
            emails = [emails]
        for email in emails:
            if email and _is_professional_email(email.lower()):
                return email.lower()
    except Exception:
        pass
    return ""


def _is_js_spa(soup: BeautifulSoup) -> bool:
    """
    Retourne True si la page ressemble à un site JavaScript non rendu :
    - Présence d'une div container SPA (id=root/app/__nuxt/__next)
    - OU texte visible < 500 caractères après nettoyage des espaces
    """
    for spa_id in ("root", "app", "__nuxt", "__next"):
        if soup.find("div", id=spa_id):
            return True
    text = " ".join(soup.get_text().split())
    return len(text) < 500


def _playwright_scrape(urls: list[str], timeout: int = 15) -> str:
    """
    Rend les pages via Playwright (headless Chromium) et extrait le premier email trouvé.
    Ouvre un seul browser pour toutes les URLs (efficace).
    Nécessite : pip install playwright && playwright install chromium
    """
    if not _PLAYWRIGHT_OK:
        return ""
    try:
        with _sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            try:
                for url in urls:
                    try:
                        page = browser.new_page(extra_http_headers=_BROWSER_HEADERS)
                        page.goto(url, timeout=timeout * 1000, wait_until="networkidle")
                        html = page.content()
                        page.close()
                        soup  = BeautifulSoup(html, "html.parser")
                        found = _extract_emails_from_page(soup, html)
                        valid = [e for e in found if _is_valid_email(e)]
                        if valid:
                            return sorted(valid, key=_email_priority)[0]
                    except Exception:
                        continue
            finally:
                browser.close()
    except Exception:
        pass
    return ""


def _is_valid_email(email: str) -> bool:
    """
    Retourne True si l'email semble légitime.
    Critères : format strict, TLD 2-6 lettres, partie locale saine,
    domaine non blacklisté, local non rejeté (système/registrar).
    """
    email = email.lower().strip()
    # Exactement un @
    if email.count("@") != 1:
        return False
    local, domain = email.split("@", 1)

    # ── Partie locale ────────────────────────────────────────────────────────
    if len(local) < 2 or len(local) > 40:
        return False
    if local.startswith(".") or local.endswith("."):
        return False
    # Caractères autorisés uniquement (pas de (, ), [, ], espaces, etc.)
    if re.search(r"[^a-z0-9._%+\-]", local):
        return False
    # Rejeter les emails système/registrar
    local_clean = local.replace("-", "").replace("_", "").replace(".", "")
    if local in _LOCAL_REJECT or local_clean in _LOCAL_REJECT:
        return False
    # Rejeter les artefacts JS/CSS (segment par segment)
    segments = re.split(r"[.\-_+]", local)
    if any(seg in _JS_CSS_LOCAL_BLACKLIST for seg in segments if len(seg) > 1):
        return False

    # ── Domaine ──────────────────────────────────────────────────────────────
    if "." not in domain:
        return False
    if re.search(r"[^a-z0-9.\-]", domain):
        return False
    # TLD : 2 à 6 lettres uniquement
    tld = domain.rsplit(".", 1)[1]
    if not re.match(r"^[a-z]{2,6}$", tld):
        return False
    if any(domain.endswith(ext) for ext in _FAKE_TLDS):
        return False
    if any(bl in domain for bl in _DOMAIN_BLACKLIST):
        return False

    return True


def _is_professional_email(email: str) -> bool:
    """
    Retourne True si l'email est valide ET n'appartient pas à un provider gratuit.
    Utilisé par la collecte massive pour ne garder que les emails d'entreprise.
    """
    if not _is_valid_email(email):
        return False
    domain = email.lower().rsplit("@", 1)[1]
    return domain not in FREE_EMAIL_DOMAINS


def _email_priority(email: str) -> int:
    """Score de priorité : 0 = email métier spécifique (meilleur), 1 = générique."""
    local = email.split("@")[0].lower()
    return 1 if any(g in local for g in _GENERIC_PREFIXES) else 0


# ── SMTP email guessing ───────────────────────────────────────────────────────

# Patterns génériques testés en priorité sur le domaine de l'entreprise
_SMTP_PATTERNS = [
    "contact", "info", "bonjour", "hello", "accueil",
    "direction", "gerant", "manager", "admin",
    "reservation", "reservations", "booking",
    "restaurant", "resto", "salon", "spa",
    "pro", "pro2",
]


def _normalize(text: str) -> str:
    """Supprime les accents et met en minuscules."""
    return unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode().lower()


def _get_mx_host(domain: str, timeout: int = 5) -> str:
    """Retourne l'hôte MX prioritaire du domaine, ou '' si introuvable."""
    if not _DNS_OK:
        return ""
    try:
        records = _dns_resolver.resolve(domain, "MX", lifetime=timeout)
        best = min(records, key=lambda r: r.preference)
        return str(best.exchange).rstrip(".")
    except Exception:
        return ""


def _smtp_exists(email: str, mx_host: str, timeout: int = 5) -> bool:
    """
    Vérifie si une boîte mail existe via SMTP (RCPT TO) sans envoyer de message.
    Retourne True si le serveur répond 250, False sinon.
    """
    try:
        with smtplib.SMTP(mx_host, 25, timeout=timeout) as smtp:
            smtp.ehlo("mail.verify.com")
            smtp.mail("check@mail.verify.com")
            code, _ = smtp.rcpt(email)
            smtp.quit()
            return code == 250
    except Exception:
        return False


def find_email_by_smtp(
    domain: str,
    prenom: str = "",
    nom: str = "",
    timeout: int = 5,
) -> str:
    """
    Tente de trouver un email valide sur un domaine via SMTP pattern guessing.

    Stratégie :
      1. Récupère le MX record du domaine
      2. Teste les patterns génériques courants (contact@, info@, accueil@…)
      3. Si prénom/nom fournis, teste aussi les patterns nominatifs
      4. Retourne le premier email confirmé ou "" si aucun

    Args:
        domain  : domaine de l'entreprise (ex: "restaurant-dupont.fr")
        prenom  : prénom du dirigeant (optionnel)
        nom     : nom du dirigeant (optionnel)
        timeout : délai SMTP en secondes

    Returns:
        Adresse email vérifiée (str) ou chaîne vide.
    """
    mx = _get_mx_host(domain, timeout)
    if not mx:
        return ""

    candidates = [f"{p}@{domain}" for p in _SMTP_PATTERNS]

    # Patterns nominatifs si on a un nom de dirigeant
    if prenom and nom:
        p = _normalize(prenom)
        n = _normalize(nom)
        candidates = [
            f"{p}.{n}@{domain}",
            f"{p[0]}.{n}@{domain}",
            f"{p}@{domain}",
            f"{n}@{domain}",
        ] + candidates

    for email in candidates:
        if _smtp_exists(email, mx, timeout):
            return email

    return ""


def scrape_email_from_website(url: str, timeout: int = 8) -> str:
    """
    Cherche une adresse email sur le site web d'un établissement.

    Pipeline (ordre de priorité) :
      1. Pour chaque page (home + pages contact) :
         <a href="mailto:"> + Cloudflare decode + JSON-LD + <meta>
      2. Playwright headless si rien trouvé (sites JavaScript, optionnel)
      3. SMTP guessing (dernier recours)

    Aucun regex HTML brut. Accepte tous les emails valides (y compris Gmail etc.)

    Args:
        url     : URL du site web (doit commencer par http/https)
        timeout : Délai max par requête HTTP en secondes

    Returns:
        Adresse email (str) ou chaîne vide.
    """
    if not url or not url.startswith("http"):
        return ""

    parsed = urlparse(url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    domain = parsed.netloc.lstrip("www.")
    pages  = [url] + [urljoin(base, p) for p in _CONTACT_PATHS]

    # ── Étape 1 : sources fiables sur toutes les pages ────────────────────────
    js_spa_detected = False
    soups_for_fallback: list[BeautifulSoup] = []

    for page_url in pages:
        try:
            resp = requests.get(
                page_url,
                headers=_BROWSER_HEADERS,
                timeout=timeout,
                allow_redirects=True,
            )
            if resp.status_code != 200:
                continue
            if "text/html" not in resp.headers.get("Content-Type", ""):
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            if page_url == url and _is_js_spa(soup):
                js_spa_detected = True
            found = _extract_emails_from_page(soup, resp.text)
            valid = [e for e in found if _is_valid_email(e)]
            if valid:
                return sorted(valid, key=_email_priority)[0]
            soups_for_fallback.append(soup)

        except requests.RequestException:
            continue

    # ── Étape 1b : fallback texte visible (emails non structurés) ─────────────
    for soup in soups_for_fallback:
        found = _extract_emails_from_visible_text(soup)
        valid = [e for e in found if _is_valid_email(e)]
        if valid:
            return sorted(valid, key=_email_priority)[0]

    # ── Étape 2 : Playwright — seulement si SPA détecté ─────────────────────
    if _PLAYWRIGHT_OK and js_spa_detected:
        contact_url = urljoin(base, "/contact")
        pw_email = _playwright_scrape([url, contact_url], timeout=15)
        if pw_email:
            return pw_email

    # ── Étape 3 : SMTP guessing ───────────────────────────────────────────────
    if domain:
        smtp_email = find_email_by_smtp(domain)
        if smtp_email and _is_valid_email(smtp_email):
            return smtp_email

    return ""


def enrich_with_emails(
    places: list[dict],
    progress_callback=None,
) -> list[dict]:
    """
    Ajoute le champ "email" à chaque lieu en visitant son site web.

    Args:
        places            : Liste de lieux (doivent avoir "websiteUri")
        progress_callback : Fonction(current, total) appelée à chaque étape

    Returns:
        Liste enrichie (modification en place).
    """
    total = len(places)
    for i, place in enumerate(places):
        website = place.get("websiteUri", "")
        place["email"] = scrape_email_from_website(website)
        if progress_callback:
            progress_callback(i + 1, total)
        time.sleep(0.2)  # Pause courtoise entre les requêtes
    return places


# ── Classe principale (recherche rapide) ──────────────────────────────────────

class GooglePlacesScraper:
    """
    Interface vers Google Places API (New).

    Usage minimal :
        scraper = GooglePlacesScraper()
        places  = scraper.search_places("restaurant", "Lyon")
        csv     = scraper.to_csv_bytes(places)
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("GOOGLE_PLACES_API_KEY", "").strip()
        if not self.api_key:
            raise ValueError(
                "Clé API introuvable. "
                "Créez un fichier .env contenant : GOOGLE_PLACES_API_KEY=votre_clé"
            )

    # ── Helpers internes ──────────────────────────────────────────────────────

    def _headers(self, field_mask: list[str]) -> dict:
        """Construit les en-têtes HTTP requis par l'API (New)."""
        return {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": ",".join(field_mask),
        }

    def _is_excluded(self, name: str, excluded: list[str]) -> bool:
        """Retourne True si le nom contient un mot-clé à exclure."""
        name_lower = name.lower()
        return any(kw in name_lower for kw in excluded)

    # ── Méthodes publiques ────────────────────────────────────────────────────

    def search_places(
        self,
        business_type: str,
        location: str,
        excluded_keywords: Optional[list[str]] = None,
        max_results: int = 60,
        min_reviews: int = 0,
        language: str = "fr",
    ) -> list[dict]:
        """
        Recherche des entreprises locales via Text Search (New).

        Gère automatiquement la pagination (jusqu'à 3 pages × 20 = 60 résultats max
        par requête — limite de l'API gratuite).

        Args:
            business_type     : Type d'activité (ex: "restaurant", "coiffeur")
            location          : Ville ou code postal (ex: "Paris", "69001")
            excluded_keywords : Noms à exclure (ex: ["McDonald's", "KFC"])
            max_results       : Nombre maximum de résultats à retourner
            min_reviews       : Filtre proxy sur la notoriété (nb d'avis minimum)
            language          : Code langue des résultats (ex: "fr", "en")

        Returns:
            Liste de dicts bruts tels que retournés par l'API.

        Raises:
            RuntimeError : Si l'API retourne une erreur HTTP.
        """
        url = f"{PLACES_API_BASE}/places:searchText"
        query = f"{business_type} {location}"

        # Normalisation des mots-clés exclus (insensible à la casse)
        excluded = [kw.lower().strip() for kw in (excluded_keywords or []) if kw.strip()]

        results: list[dict] = []
        seen_ids: set[str] = set()
        page_token: Optional[str] = None

        while len(results) < max_results:
            payload: dict = {
                "textQuery": query,
                "maxResultCount": 20,    # Limite fixe de l'API par page
                "languageCode": language,
            }
            if page_token:
                payload["pageToken"] = page_token

            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers=self._headers(TEXT_SEARCH_FIELDS),
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
            except requests.HTTPError as exc:
                # Remonter le message d'erreur de l'API pour faciliter le debug
                raise RuntimeError(
                    f"Erreur Google Places API ({exc.response.status_code}) : "
                    f"{exc.response.text}"
                ) from exc
            except requests.RequestException as exc:
                raise RuntimeError(f"Erreur réseau : {exc}") from exc

            places = data.get("places", [])
            page_token = data.get("nextPageToken")

            for place in places:
                place_id = place.get("id")
                if not place_id or place_id in seen_ids:
                    continue  # Dédoublonnage

                name = place.get("displayName", {}).get("text", "")

                # ── Filtres ──────────────────────────────────────────────────
                if self._is_excluded(name, excluded):
                    continue

                # Le nombre d'avis sert de proxy pour la « taille » de l'entreprise
                # (l'API Google Places ne fournit pas le nombre d'employés)
                if min_reviews > 0 and place.get("userRatingCount", 0) < min_reviews:
                    continue

                seen_ids.add(place_id)
                results.append(place)

            # Arrêt si plus de pages disponibles ou page vide
            if not page_token or not places:
                break

            # Pause courtoise entre les pages pour éviter le rate limiting
            time.sleep(0.3)

        return results[:max_results]

    def get_place_details(self, place_id: str, language: str = "fr") -> dict:
        """
        Récupère les détails complets d'un lieu (Place Details New).

        Utile pour enrichir les données du Text Search (ex: téléphone manquant).

        Args:
            place_id : Identifiant Google du lieu
            language : Code langue

        Returns:
            Dict des détails ou dict vide en cas d'erreur.
        """
        url = f"{PLACES_API_BASE}/places/{place_id}"

        try:
            response = requests.get(
                url,
                headers=self._headers(DETAIL_FIELDS),
                params={"languageCode": language},
                timeout=15,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            # On retourne un dict vide plutôt que de planter l'export entier
            return {}

    def enrich_with_details(
        self,
        places: list[dict],
        language: str = "fr",
        progress_callback=None,
    ) -> list[dict]:
        """
        Complète chaque lieu avec un appel Place Details individuel.

        À utiliser si le Text Search ne remonte pas le téléphone ou le site web.
        Attention : chaque appel est facturé séparément par Google.

        Args:
            places            : Liste de lieux issus de search_places()
            language          : Code langue
            progress_callback : Fonction appelée avec (index, total) à chaque étape

        Returns:
            Liste enrichie (modification en place + retour).
        """
        total = len(places)
        for i, place in enumerate(places):
            place_id = place.get("id")
            if place_id:
                details = self.get_place_details(place_id, language)
                # On fusionne les détails dans le dict existant
                place.update(details)
            if progress_callback:
                progress_callback(i + 1, total)
            time.sleep(0.2)  # Pause entre les appels details
        return places

    # ── Formatage et export ───────────────────────────────────────────────────

    def format_for_csv(self, places: list[dict]) -> list[dict]:
        """
        Convertit les dicts bruts de l'API en lignes prêtes pour le CSV.

        Returns:
            Liste de dicts avec les clés définies dans CSV_COLUMNS.
        """
        rows = []
        for place in places:
            # Préférer le numéro national, sinon international
            phone = place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber", "")

            rows.append({
                "nom":          place.get("displayName", {}).get("text", ""),
                "adresse":      place.get("formattedAddress", ""),
                "telephone":    phone,
                "email":        place.get("email", ""),
                "site_web":     place.get("websiteUri", ""),
                "note_google":  place.get("rating", ""),
                "nombre_avis":  place.get("userRatingCount", ""),
                "statut":       place.get("businessStatus", ""),
                "types":        " | ".join(place.get("types", [])),
            })
        return rows

    def to_csv_bytes(self, places: list[dict]) -> bytes:
        """
        Exporte les lieux en CSV encodé UTF-8 avec BOM.

        Le BOM (utf-8-sig) garantit l'ouverture correcte dans Excel sur Windows.

        Returns:
            Bytes du fichier CSV, ou b"" si la liste est vide.
        """
        rows = self.format_for_csv(places)
        if not rows:
            return b""

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=CSV_COLUMNS,
            extrasaction="ignore",   # Ignore les clés supplémentaires éventuelles
            lineterminator="\r\n",   # CRLF standard Windows/Excel
        )
        writer.writeheader()
        writer.writerows(rows)

        return output.getvalue().encode("utf-8-sig")


# ── Collecteur massif ──────────────────────────────────────────────────────────

class MassiveCollector:
    """
    Collecteur massif d'entreprises locales pour toute la France.

    Lance la collecte dans un thread daemon et expose un état thread-safe
    pour l'interface Streamlit (polling toutes les ~1s).

    Usage :
        collector = MassiveCollector("collecte_france.csv", ["restauration"], None)
        collector.start()
        while collector.is_running:
            state = collector.get_state()
            print(state["total_found"])
            time.sleep(1)
    """

    def __init__(
        self,
        output_path: str,
        sectors: list[str],
        dept_filter: Optional[list[str]],
        language: str = "fr",
    ) -> None:
        self.output_path = Path(output_path)
        self.sectors = sectors
        self.dept_filter = dept_filter  # None = toute la France
        self.language = language

        # ── Clé API ───────────────────────────────────────────────────────────
        self._api_key = os.getenv("GOOGLE_PLACES_API_KEY", "").strip()
        if not self._api_key:
            raise ValueError("Clé API Google Places introuvable dans .env")

        # ── État partagé (lock pour accès thread-safe) ────────────────────────
        self._lock = threading.Lock()
        self._state: dict = {
            "is_running":        False,
            "is_done":           False,
            "progress":          0.0,
            "total_combinations": 0,
            "done_combinations": 0,
            "total_found":       0,
            "with_website":      0,
            "with_email":        0,
            "api_calls":         0,
            "current_task":      "",
            "last_results":      [],   # 50 derniers résultats pour l'UI
            "error":             None,
        }

        # ── Contrôle du thread ────────────────────────────────────────────────
        self.stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # ── Place IDs déjà traités (pour la reprise) ─────────────────────────
        self._seen_ids: set[str] = set()

    # ── API publique ──────────────────────────────────────────────────────────

    def load_existing(self) -> int:
        """
        Charge les place_ids du CSV existant pour reprendre une collecte.
        Retourne le nombre de lignes chargées.
        """
        if not self.output_path.exists():
            return 0
        count = 0
        try:
            with open(self.output_path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pid = row.get("place_id", "").strip()
                    if pid:
                        self._seen_ids.add(pid)
                        count += 1
        except Exception:
            pass
        with self._lock:
            self._state["total_found"] = count
        return count

    def start(self) -> None:
        """Lance la collecte dans un thread daemon."""
        if self.is_running:
            return
        self.stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Demande l'arrêt propre du thread de collecte."""
        self.stop_event.set()

    def get_state(self) -> dict:
        """Retourne une copie de l'état courant (thread-safe)."""
        with self._lock:
            state = dict(self._state)
            state["last_results"] = list(self._state["last_results"])
            return state

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ── Internals ─────────────────────────────────────────────────────────────

    def _update_state(self, **kwargs) -> None:
        with self._lock:
            self._state.update(kwargs)

    def _api_headers(self, field_mask: list[str]) -> dict:
        return {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self._api_key,
            "X-Goog-FieldMask": ",".join(field_mask),
        }

    def _search_one_combo(
        self, city: str, business_type: str, dept_name: str, sector: str
    ) -> list[dict]:
        """
        Recherche toutes les pages disponibles pour une combinaison ville × type.
        Retourne la liste des places nouvelles (non vues).
        """
        url = f"{PLACES_API_BASE}/places:searchText"
        query = f"{business_type} {city} France"

        results: list[dict] = []
        page_token: Optional[str] = None

        while True:
            if self.stop_event.is_set():
                break

            payload: dict = {
                "textQuery": query,
                "maxResultCount": 20,
                "languageCode": self.language,
            }
            if page_token:
                payload["pageToken"] = page_token

            try:
                resp = requests.post(
                    url,
                    json=payload,
                    headers=self._api_headers(TEXT_SEARCH_FIELDS),
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException:
                break

            with self._lock:
                self._state["api_calls"] += 1

            for place in data.get("places", []):
                pid = place.get("id")
                if not pid:
                    continue
                with self._lock:
                    if pid in self._seen_ids:
                        continue
                    self._seen_ids.add(pid)

                # Métadonnées de contexte (prefixe _ pour ne pas polluer le CSV direct)
                place["_ville"] = city
                place["_dept"] = dept_name
                place["_secteur"] = sector
                place["_type_business"] = business_type
                results.append(place)

            page_token = data.get("nextPageToken")
            if not page_token or not data.get("places"):
                break

            time.sleep(0.5)  # Pause entre les pages

        return results

    def _scrape_emails_batch(self, places: list[dict]) -> list[dict]:
        """
        Scrape les emails en parallèle pour une liste de lieux.
        N'applique que les emails professionnels (filtre FREE_EMAIL_DOMAINS).
        """
        to_scrape = [(i, p) for i, p in enumerate(places) if p.get("websiteUri")]
        if not to_scrape:
            return places

        def _scrape_one(args: tuple) -> tuple:
            idx, place = args
            email = scrape_email_from_website(place["websiteUri"], timeout=5)
            return idx, email

        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            futures = {executor.submit(_scrape_one, arg): arg for arg in to_scrape}
            for future in concurrent.futures.as_completed(futures):
                try:
                    idx, email = future.result()
                    places[idx]["email"] = email
                except Exception:
                    pass

        return places

    def _append_to_csv(self, places: list[dict]) -> None:
        """Ajoute les résultats au CSV (crée le fichier avec header si absent)."""
        rows = []
        for place in places:
            phone = (
                place.get("nationalPhoneNumber")
                or place.get("internationalPhoneNumber", "")
            )
            rows.append({
                "place_id":      place.get("id", ""),
                "nom":           place.get("displayName", {}).get("text", ""),
                "adresse":       place.get("formattedAddress", ""),
                "telephone":     phone,
                "email":         place.get("email", ""),
                "site_web":      place.get("websiteUri", ""),
                "note_google":   place.get("rating", ""),
                "nombre_avis":   place.get("userRatingCount", ""),
                "statut":        place.get("businessStatus", ""),
                "types":         " | ".join(place.get("types", [])),
                "ville":         place.get("_ville", ""),
                "departement":   place.get("_dept", ""),
                "secteur":       place.get("_secteur", ""),
                "type_business": place.get("_type_business", ""),
            })

        if not rows:
            return

        file_exists = self.output_path.exists()
        with open(self.output_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=CSV_COLUMNS_MASSIVE,
                extrasaction="ignore",
                lineterminator="\r\n",
            )
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)

    def _format_for_display(self, places: list[dict]) -> list[dict]:
        """Formate les places pour l'affichage dans le tableau Streamlit."""
        rows = []
        for p in places:
            phone = (
                p.get("nationalPhoneNumber")
                or p.get("internationalPhoneNumber", "")
            )
            rows.append({
                "nom":      p.get("displayName", {}).get("text", ""),
                "ville":    p.get("_ville", ""),
                "secteur":  p.get("_secteur", ""),
                "email":    p.get("email", ""),
                "telephone": phone,
                "site_web": p.get("websiteUri", ""),
                "note":     p.get("rating", ""),
            })
        return rows

    def _run(self) -> None:
        """
        Boucle principale de collecte (exécutée dans le thread daemon).
        Pour chaque combinaison ville × type_business :
          1. Appel(s) Google Places API (avec pagination)
          2. Scraping emails en parallèle
          3. Sauvegarde CSV incrémentale
          4. Mise à jour de l'état
        """
        # ── Construction de la liste des combinaisons ─────────────────────────
        cities = [
            (city, dept, dept_name)
            for city, dept, dept_name in FRENCH_CITIES
            if self.dept_filter is None or dept in self.dept_filter
        ]

        all_type_combos: list[tuple[str, str]] = []
        for sector_key in self.sectors:
            for btype in SECTORS[sector_key]["types"]:
                all_type_combos.append((sector_key, btype))

        combinations = [
            (city, dept, dept_name, sector_key, btype)
            for city, dept, dept_name in cities
            for sector_key, btype in all_type_combos
        ]

        total = len(combinations)
        self._update_state(is_running=True, total_combinations=total)

        for i, (city, dept, dept_name, sector_key, btype) in enumerate(combinations):
            if self.stop_event.is_set():
                break

            self._update_state(
                current_task=f"{city} ({dept}) — {btype}",
                progress=i / total if total else 0.0,
                done_combinations=i,
            )

            # ── Recherche Google Places ───────────────────────────────────────
            try:
                new_places = self._search_one_combo(city, btype, dept_name, sector_key)
            except Exception as exc:
                self._update_state(error=str(exc))
                time.sleep(0.5)
                continue

            if not new_places:
                time.sleep(0.5)
                continue

            # ── Comptage sites web avant scraping ─────────────────────────────
            with_website = sum(1 for p in new_places if p.get("websiteUri"))

            # ── Scraping emails en parallèle ──────────────────────────────────
            new_places = self._scrape_emails_batch(new_places)

            with_email = sum(1 for p in new_places if p.get("email"))

            # ── Sauvegarde CSV ────────────────────────────────────────────────
            try:
                self._append_to_csv(new_places)
            except Exception as exc:
                self._update_state(error=f"Erreur écriture CSV : {exc}")

            # ── Mise à jour état global ───────────────────────────────────────
            formatted = self._format_for_display(new_places)
            with self._lock:
                self._state["total_found"] += len(new_places)
                self._state["with_website"] += with_website
                self._state["with_email"] += with_email
                # Garder les 50 derniers résultats pour l'UI
                self._state["last_results"] = (
                    self._state["last_results"] + formatted
                )[-50:]

            time.sleep(0.5)  # Pause courtoise entre les appels Google

        # ── Fin de la boucle ──────────────────────────────────────────────────
        self._update_state(
            is_running=False,
            is_done=not self.stop_event.is_set(),
            progress=1.0,
            done_combinations=total,
        )


