import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Auto Parts - Support & Survey Intelligence", layout="wide")

session = get_active_session()


@st.cache_data(ttl=60)
def load_tickets():
    return session.sql("SELECT * FROM AI_DEMOS.PUBLIC.AUTO_PARTS_TICKETS_ENRICHED ORDER BY SUBMITTED_AT DESC").to_pandas()


@st.cache_data(ttl=60)
def load_surveys():
    return session.sql("SELECT * FROM AI_DEMOS.PUBLIC.AUTO_PARTS_SURVEYS_ANALYZED ORDER BY SUBMITTED_AT DESC").to_pandas()


st.title("Auto Parts Support & Survey Intelligence")
st.caption("Powered by Snowflake Cortex AI  |  Dynamic Tables for real-time enrichment")

tab1, tab2, tab3, tab4 = st.tabs(
    ["Support Tickets", "Survey Analysis", "Vehicle Insights", "AI Playground"]
)

# ── TAB 1: SUPPORT TICKET INTELLIGENCE ──
with tab1:
    tickets = load_tickets()

    # Sidebar filters scoped to tickets
    with st.sidebar:
        st.header("Filters")
        cats = ["All"] + sorted(tickets["CATEGORY"].dropna().unique().tolist())
        sel_cat = st.selectbox("Ticket Category", cats)
        urgencies = ["All"] + sorted(tickets["URGENCY"].dropna().unique().tolist())
        sel_urg = st.selectbox("Urgency", urgencies)
        channels = ["All"] + sorted(tickets["CHANNEL"].dropna().unique().tolist())
        sel_chan = st.selectbox("Channel", channels)

    filtered = tickets.copy()
    if sel_cat != "All":
        filtered = filtered[filtered["CATEGORY"] == sel_cat]
    if sel_urg != "All":
        filtered = filtered[filtered["URGENCY"] == sel_urg]
    if sel_chan != "All":
        filtered = filtered[filtered["CHANNEL"] == sel_chan]

    # KPIs
    total = len(filtered)
    avg_sent = filtered["SENTIMENT_SCORE"].mean() if total > 0 else 0
    high_crit = len(filtered[filtered["URGENCY"].isin(["High", "Critical"])]) if total > 0 else 0
    high_crit_pct = (high_crit / total * 100) if total > 0 else 0
    top_cat = filtered["CATEGORY"].mode().iloc[0] if total > 0 else "N/A"

    with st.container(horizontal=True):
        st.metric("Total Tickets", f"{total}", border=True)
        st.metric("Avg Sentiment", f"{avg_sent:.2f}", border=True)
        st.metric("High/Critical %", f"{high_crit_pct:.0f}%", border=True)
        st.metric("Top Category", top_cat, border=True)

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.subheader("Tickets by Category")
            cat_counts = filtered["CATEGORY"].value_counts().reset_index()
            cat_counts.columns = ["Category", "Count"]
            st.bar_chart(cat_counts, x="Category", y="Count", horizontal=True)

    with col2:
        with st.container(border=True):
            st.subheader("Tickets by Urgency")
            urg_counts = filtered["URGENCY"].value_counts().reset_index()
            urg_counts.columns = ["Urgency", "Count"]
            st.bar_chart(urg_counts, x="Urgency", y="Count", color="Urgency")

    col3, col4 = st.columns(2)

    with col3:
        with st.container(border=True):
            st.subheader("Tickets by Channel")
            chan_counts = filtered["CHANNEL"].value_counts().reset_index()
            chan_counts.columns = ["Channel", "Count"]
            st.bar_chart(chan_counts, x="Channel", y="Count")

    with col4:
        with st.container(border=True):
            st.subheader("Sentiment Distribution")
            import pandas as pd

            bins = pd.cut(
                filtered["SENTIMENT_SCORE"],
                bins=[-1.1, -0.5, -0.1, 0.1, 0.5, 1.1],
                labels=["Very Negative", "Negative", "Neutral", "Positive", "Very Positive"],
            )
            sent_dist = bins.value_counts().reset_index()
            sent_dist.columns = ["Sentiment", "Count"]
            st.bar_chart(sent_dist, x="Sentiment", y="Count")

    with st.container(border=True):
        st.subheader("Enriched Ticket Details")
        display_cols = [
            "TICKET_ID", "SUBMITTED_AT", "CUSTOMER_NAME", "VEHICLE_MAKE",
            "VEHICLE_MODEL", "CATEGORY", "URGENCY", "SENTIMENT_SCORE",
            "SUMMARY", "CHANNEL",
        ]
        st.dataframe(
            filtered[display_cols],
            hide_index=True,
            use_container_width=True,
            column_config={
                "SENTIMENT_SCORE": st.column_config.ProgressColumn(
                    "Sentiment", min_value=-1, max_value=1, format="%.2f"
                ),
                "SUBMITTED_AT": st.column_config.DatetimeColumn("Date", format="MMM DD, YYYY"),
            },
        )

# ── TAB 2: SURVEY ANALYSIS ──
with tab2:
    surveys = load_surveys()

    total_s = len(surveys)
    avg_rating = surveys["RATING"].mean() if total_s > 0 else 0
    avg_sent_s = surveys["SENTIMENT_SCORE"].mean() if total_s > 0 else 0
    promoters = len(surveys[surveys["RATING"] >= 4])
    detractors = len(surveys[surveys["RATING"] <= 2])
    nps_proxy = ((promoters - detractors) / total_s * 100) if total_s > 0 else 0

    with st.container(horizontal=True):
        st.metric("Total Surveys", f"{total_s}", border=True)
        st.metric("Avg Rating", f"{avg_rating:.1f} / 5", border=True)
        st.metric("Avg Sentiment", f"{avg_sent_s:.2f}", border=True)
        st.metric("NPS Proxy", f"{nps_proxy:+.0f}", border=True)

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.subheader("Surveys by Category")
            scat = surveys["CATEGORY"].value_counts().reset_index()
            scat.columns = ["Category", "Count"]
            st.bar_chart(scat, x="Category", y="Count", horizontal=True)

    with col2:
        with st.container(border=True):
            st.subheader("Rating Distribution")
            import pandas as pd

            rating_dist = surveys["RATING"].value_counts().sort_index().reset_index()
            rating_dist.columns = ["Rating", "Count"]
            rating_dist["Rating"] = rating_dist["Rating"].apply(lambda x: f"{x} Star")
            st.bar_chart(rating_dist, x="Rating", y="Count")

    col3, col4 = st.columns(2)

    with col3:
        with st.container(border=True):
            st.subheader("Sentiment by Survey Type")
            by_type = surveys.groupby("SURVEY_TYPE")["SENTIMENT_SCORE"].mean().reset_index()
            by_type.columns = ["Survey Type", "Avg Sentiment"]
            st.bar_chart(by_type, x="Survey Type", y="Avg Sentiment")

    with col4:
        with st.container(border=True):
            st.subheader("Rating by Category")
            by_cat_rating = surveys.groupby("CATEGORY")["RATING"].mean().reset_index()
            by_cat_rating.columns = ["Category", "Avg Rating"]
            st.bar_chart(by_cat_rating, x="Category", y="Avg Rating", horizontal=True)

    with st.container(border=True):
        st.subheader("Survey Details")
        st.dataframe(
            surveys[["SURVEY_ID", "SUBMITTED_AT", "CUSTOMER_NAME", "RATING", "SURVEY_TYPE", "CATEGORY", "SENTIMENT_SCORE", "KEY_THEMES"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "RATING": st.column_config.NumberColumn("Rating", format="%d ⭐"),
                "SENTIMENT_SCORE": st.column_config.ProgressColumn(
                    "Sentiment", min_value=-1, max_value=1, format="%.2f"
                ),
                "SUBMITTED_AT": st.column_config.DatetimeColumn("Date", format="MMM DD, YYYY"),
            },
        )

# ── TAB 3: VEHICLE INSIGHTS ──
with tab3:
    tickets = load_tickets()

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.subheader("Tickets by Vehicle Make")
            make_counts = tickets["VEHICLE_MAKE"].value_counts().reset_index()
            make_counts.columns = ["Make", "Count"]
            st.bar_chart(make_counts, x="Make", y="Count", horizontal=True)

    with col2:
        with st.container(border=True):
            st.subheader("Avg Sentiment by Make")
            make_sent = tickets.groupby("VEHICLE_MAKE")["SENTIMENT_SCORE"].mean().reset_index()
            make_sent.columns = ["Make", "Avg Sentiment"]
            make_sent = make_sent.sort_values("Avg Sentiment")
            st.bar_chart(make_sent, x="Make", y="Avg Sentiment", horizontal=True)

    with st.container(border=True):
        st.subheader("Top Issues by Vehicle Make")
        make_cat = (
            tickets.groupby(["VEHICLE_MAKE", "CATEGORY"])
            .size()
            .reset_index(name="Count")
            .sort_values(["VEHICLE_MAKE", "Count"], ascending=[True, False])
        )
        sel_make = st.selectbox("Select Make", sorted(tickets["VEHICLE_MAKE"].unique()))
        make_filtered = make_cat[make_cat["VEHICLE_MAKE"] == sel_make]
        st.bar_chart(make_filtered, x="CATEGORY", y="Count")

    with st.container(border=True):
        st.subheader("Urgency Breakdown by Make")
        make_urg = (
            tickets.groupby(["VEHICLE_MAKE", "URGENCY"])
            .size()
            .reset_index(name="Count")
        )
        st.bar_chart(make_urg, x="VEHICLE_MAKE", y="Count", color="URGENCY", stack=True)

# ── TAB 4: AI PLAYGROUND ──
with tab4:
    st.subheader("Live Cortex AI Demo")
    st.markdown(
        "Enter any customer support text below to see Snowflake Cortex AI analyze it in real time."
    )

    sample_text = "The brake pads for my 2020 BMW X3 wore out after just 4 months. I need a warranty replacement shipped overnight - my car is unsafe to drive."

    user_text = st.text_area("Enter support ticket or survey text:", value=sample_text, height=120)

    if st.button("Analyze with Cortex AI", type="primary"):
        with st.spinner("Running Cortex AI functions..."):
            col1, col2 = st.columns(2)

            with col1:
                with st.container(border=True):
                    st.markdown("**Classification**")
                    result = session.sql(f"""
                        SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                            $${user_text}$$,
                            ['Warranty/Returns', 'Shipping Issue', 'Wrong Fitment', 'Product Quality', 'Pricing', 'Technical Question', 'General Inquiry', 'Positive Feedback'],
                            {{'task_description': 'Classify an auto parts customer support ticket'}}
                        ):label::VARCHAR AS CATEGORY
                    """).collect()
                    st.success(f"Category: **{result[0]['CATEGORY']}**")

                with st.container(border=True):
                    st.markdown("**Sentiment**")
                    result = session.sql(f"""
                        SELECT ROUND(SNOWFLAKE.CORTEX.SENTIMENT($${user_text}$$), 4) AS SCORE
                    """).collect()
                    score = float(result[0]["SCORE"])
                    label = "Positive" if score > 0.1 else ("Negative" if score < -0.1 else "Neutral")
                    st.metric("Score", f"{score:.4f}")
                    st.info(f"Interpretation: **{label}**")

            with col2:
                with st.container(border=True):
                    st.markdown("**Urgency**")
                    result = session.sql(f"""
                        SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                            $${user_text}$$,
                            ['Low', 'Medium', 'High', 'Critical'],
                            {{'task_description': 'Classify urgency of this customer support ticket based on safety risk, time sensitivity, and frustration'}}
                        ):label::VARCHAR AS URGENCY
                    """).collect()
                    urg = result[0]["URGENCY"]
                    color_map = {"Low": "green", "Medium": "orange", "High": "red", "Critical": "red"}
                    st.success(f"Urgency: **{urg}**")

                with st.container(border=True):
                    st.markdown("**Summary**")
                    result = session.sql(f"""
                        SELECT SNOWFLAKE.CORTEX.SUMMARIZE($${user_text}$$) AS SUMMARY
                    """).collect()
                    st.write(result[0]["SUMMARY"])

            with st.container(border=True):
                st.markdown("**Translation (auto-detect to English)**")
                result = session.sql(f"""
                    SELECT SNOWFLAKE.CORTEX.TRANSLATE($${user_text}$$, '', 'en') AS TRANSLATED
                """).collect()
                st.write(result[0]["TRANSLATED"])
