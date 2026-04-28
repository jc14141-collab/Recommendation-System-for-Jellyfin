# Recommendation System — User Guide

## Overview

This system is a movie recommendation engine fully integrated into Jellyfin. Users can receive personalized recommendations, provide feedback through multiple channels, and watch movies — all within the Jellyfin interface. 


---

## Access

1. Open the Jellyfin web interface: `http://<server-ip>:8096`
2. Log in with your credentials
3. Click **Recommend** in the top navigation bar

> **Note on the movie library:** Due to storage constraints and copyright considerations, only a small number of sample movies have been uploaded to this server. This is how Jellyfin works by design — in a real deployment, users would import their own movie library. You can still explore all recommendation features regardless of the local library size.

---

## Recommendations Page (`#/recommend`)

### Model & Top N Selection

At the top of the page, two controls let you configure the recommendation:

| Control | Options |
|---|---|
| **Model** | `latest (MLP · recommended)` / `v0002` / `v0001` / `LightGBM` / `MLP Large` |
| **Top N** | `10` / `20` / `50` |

Click **Refresh** (or change either dropdown) to reload recommendations for the current user.

Each recommendation card shows:
- Movie poster (fetched from TMDB)
- Title, genre tags, and predicted score
- A **Like** button

---

### Like Button

Clicking **Like** on a card does two things:
1. Sends a feedback signal (`/api/feedback`) recording which movie was clicked and its rank in the list
2. Submits a watch event (`/api/ingest-event`) to update the user embedding

Click **Refresh** after liking one or more movies to see the recommendations update.

---

### Submit Preference

Use this to manually record a watch event for any movie.

**Steps:**
1. Type a movie title in the **Search movie title…** box — a dropdown will appear with matches from the MovieLens database
2. Select the movie; the matched ID is shown below the search box
3. Alternatively, type a MovieLens movie ID directly in the **Movie ID** field
4. Enter a watch duration in seconds in the **Duration (sec)** field
5. Click **Submit**

After submitting, click **Refresh** to see updated recommendations.

---

### Sync Watch History

This button reads the user's real playback history from Jellyfin and batch-submits it to the recommendation system.

**Steps:**
1. Click **Sync Watch History**
2. A progress bar shows how many movies have been processed (e.g. `12 / 15`)
3. When complete, the bar shows how many were successfully matched to MovieLens IDs
4. Click **Refresh** to see updated recommendations

The sync captures both **fully watched** movies and **partially watched** ones (any session where playback was started). Watch duration is taken directly from Jellyfin's playback position data.

> **Note:** Real user interactions update the user embedding for inference in real time, but are not written back into the core training database. This means synced history influences your recommendations immediately, but does not affect model retraining.

---

## Model Management Page (`#/recommend/admin`)

Click the **Model Management** button on the recommendation page to access admin controls.

---

### Current Model Overview

Four metric cards show the active model's status:

| Metric | Description |
|---|---|
| **Model Version** | Identifier of the currently deployed model |
| **Best Val MSE** | Validation mean squared error |
| **Hit Rate@10** | Fraction of users for whom a relevant item appears in the top 10 |
| **NDCG@10** | Normalized Discounted Cumulative Gain at rank 10 |

---

### Retrain Model

1. Select a **Dataset Version** from the dropdown
2. Select a **Base Model**:
   - `MLP (512-256-128)` — standard model
   - `MLP Large (1024-512-256-128)` — higher capacity
   - `LightGBM` — inference only, retraining disabled
3. Click **⚡ Retrain Now**

A status message confirms the training job has been submitted.

---

### Training Logs

The log panel polls every 2 seconds and streams live output from the training job. Log lines are color-coded by status (running / success / error).

---

### Model History

A table lists all past training runs with:
- Version identifier
- Model type and dataset used
- Val MSE, Hit Rate@10, NDCG@10
- Timestamp
- MLflow run link
- **Rollback** button — redeploys that version as the active model

---

### Scheduled Retraining

Configure automatic periodic retraining:

1. Toggle **Auto Retrain** on
2. Select a **Frequency** (daily / weekly / monthly)
3. Set the **Time** for the job to run
4. Click **Save Schedule**

Settings are persisted to the backend.

