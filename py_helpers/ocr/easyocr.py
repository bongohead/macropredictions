import pandas as pd
from typing import Union
from PIL import Image, ImageDraw, ImageFont
import numpy as np
import cv2

def easyocr_to_df(easyocr_output: list[tuple]) -> pd.DataFrame:
    """
    Description:
    	Convenience function to cast easyOCR outputs (bounding box coordinates, text, and probability)
        into a pandas dataframe.
    
    Params:
        @easyocr_output: A lists of tuples returned from easyocr's readtext() method.
        
    Example:
        from PIL import Image
        import easyocr

        reader = easyocr.Reader(['en'], gpu = False)
        img = Image.open('test.png')
        out = reader.readtext(image = np.array(img))

        df = easyocr_to_df(out)
    """
    df = pd.DataFrame(
        [{
            'tl_x': r[0][0][0], 'tl_y': r[0][0][1], 'tr_x': r[0][1][0], 'tr_y': r[0][1][1],
            'br_x': r[0][2][0], 'br_y': r[0][2][1],'bl_x': r[0][3][0], 'bl_y': r[0][3][1],
            'text': r[1], 'prob': r[2]
        } for r in easyocr_output]
        ).assign(
            ix = lambda df: [i for i in range(1, len(df) + 1)],
            c_x = lambda df: .25 * (df['tl_x'] + df['tr_x'] + df['bl_x'] + df['br_x']),
            c_y = lambda df: .25 * (df['tl_y'] + df['tr_y'] + df['bl_y'] + df['br_y'])
        )
    
    return df

def draw_easyocr_output(img: Union[np.ndarray, Image.Image], easyocr_df: pd.DataFrame, path: str) -> bool:
    """
    Description:
    	Convenience function to save a drawing of an easyOCR output with bounding boxes and text.
    
    Params:
        @img: The image used to create the easyOCR output; either a PIL object or a
         numpy array representing the image.
        @easyocr_df: A dataframe of easyOCR outputs, see easyocr_to_df().
        @path: The path to save the file to.

    Example:
        from PIL import Image
        import easyocr

        reader = easyocr.Reader(['en'], gpu = False)
        img = Image.open('test.png')
        out = reader.readtext(image = np.array(img))

        df = easyocr_to_df(out)
        draw_easyocr_output('out.png')
    """

    if isinstance(img, Image.Image):
        img_draw = img.copy()
    else:
        img_draw = Image.fromarray(img).copy()

    draw = ImageDraw.Draw(img_draw)
    for row in easyocr_df.to_dict('records'):
        quad = [
            (row['tl_x'], row['tl_y']), (row['tr_x'], row['tr_y']), 
            (row['br_x'], row['br_y']), (row['bl_x'], row['bl_y'])
        ]
        draw.polygon(quad, outline = 'red')
        draw.text(
            (int(row['tl_x']) - 10, int(row['tl_y']) - 10), str(row['ix']) + '=' + str(row['text']), 
            fill = 'red', font = ImageFont.load_default(size = 20)
            )
        
    img_draw.save(path)


def get_bb_darkness(
    img: Union[np.ndarray, Image.Image], 
    tl_x: int, tl_y: int, tr_x: int, tr_y: int, br_x: int, br_y: int, bl_x:int , bl_y: int,
    threshold: int = 200
    ) -> float:
    """
    Given a grayscale image, get the average background color within a bounding box

    Params:
        @img: The image used to create the easyOCR output; either a PIL object or a
         numpy array representing the image.
        @tl_x, tl_y, ...: The pixel coordinates for the top-left x coordinate, top-left y coordinate, etc.
         of the bounding box.
        @threshold: The maximum lightness for which all darker pixels than this will be not included in the
         background color calculation.
    """
    if isinstance(img, Image.Image):
        img_np = np.array(img)
    else:
        img_np = img

    coords = np.array([[tl_x, tl_y], [tr_x, tr_y], [br_x, br_y], [bl_x, bl_y]], dtype = np.int32)
    
    # Create a mask for the region of interest
    mask = np.zeros_like(img_np)
    cv2.fillPoly(mask, [coords], 1) # Fill 1s
    masked_img = img_np * mask # Replace original image to 0s everywhere except original
    
    # Get original mask region
    masked_pixels = masked_img[mask == 1] # Get original mask region
    darkness_resid = masked_pixels[masked_pixels > threshold] # Remove very dark regions
    
    return(np.mean(darkness_resid))
