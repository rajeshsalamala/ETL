from pathlib import Path
import streamlit as st
from PIL import Image

# --- PATH SETTINGS ---
current_dir = Path(__file__).parent if "__file__" in locals() else Path.cwd()
flow_pic = current_dir / "images" / "flow.png"

# --- PAGE CONFIG ---
st.set_page_config(page_title="Image Viewer", page_icon="📷", layout='wide')

# --- CHECK IF IMAGE EXISTS ---
if flow_pic.exists():
    image = Image.open(flow_pic)
    st.image(image, caption="Flow Diagram", use_column_width=True)
else:
    st.warning(f"Image not found at: {flow_pic}")
