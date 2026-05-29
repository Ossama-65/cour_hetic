"""
conftest.py — Configuration pytest pour le projet SPOTIFY

Ce fichier est automatiquement chargé par pytest.
Il configure le path Python pour que les imports src/ fonctionnent.
"""
import sys
import os

# Ajouter la racine du projet au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
