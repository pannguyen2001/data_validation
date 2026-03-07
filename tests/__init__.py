import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(BASE_DIR))
print(BASE_DIR)