from EL_pipeline import WeatherELPipeline

if __name__ == "__main__":
    pipeline = WeatherELPipeline()
    
    print("--- STARTING WEATHER EL PIPELINE ---")
    
    raw_data = pipeline.fetch_weather("Kyiv")
    
    if raw_data:
        pipeline.insert_raw_data(raw_data)
        print("--- PIPELINE SUCCESSFULLY FINISHED ---")
    else:
        print("--- PIPELINE FAILED (No data extracted) ---")