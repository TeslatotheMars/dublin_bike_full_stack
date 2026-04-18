"""
CityFlow — Chatbot Backend
===========================
Pre-written Q&A — no API key needed.
10 questions across 5 categories, all answers prepared in advance.
 
Routes:
    GET  /api/chat/categories   — returns all categories + questions
    POST /api/chat/answer       — returns the answer for a question_id
"""
 
from flask import Blueprint, jsonify, request
 
chat_bp = Blueprint("chat", __name__)
 
#  PRE-WRITTEN Q&A  —  5 categories, 2 questions each = 10 total

QA = {
    "categories": [
        {
            "id": "getting_started",
            "label": "Getting Started",
            "icon": "🚲",
            "questions": [
                {
                    "id": "gs_1",
                    "question": "How do I rent a Dublin Bike?",
                    "answer": "Renting is simple! Walk up to any station terminal, insert your card or use your subscription card, select a bike slot number and the bike releases automatically. The first 30 minutes of every journey are free! 🎉"
                },
                {
                    "id": "gs_2",
                    "question": "Do I need to register to use Dublin Bikes?",
                    "answer": "No registration needed for a casual ride — just use your credit or debit card at any terminal for a 1-day (€1.50) or 3-day (€3.50) pass. For regular use, sign up for the Annual Subscription (€35/year) at dublinbikes.ie for much better value. 🪪"
                }
            ]
        },
        {
            "id": "pricing",
            "label": "Pricing & Subscriptions",
            "icon": "💳",
            "questions": [
                {
                    "id": "pr_1",
                    "question": "How much does Dublin Bikes cost?",
                    "answer": "Three pricing options:\n\n• Annual Subscription — €35/year (best value!)\n• 3-Day Pass — €3.50\n• 1-Day Pass — €1.50\n\nThe first 30 minutes of every journey are always free regardless of your plan. 💰"
                },
                {
                    "id": "pr_2",
                    "question": "What happens if I go over 30 minutes?",
                    "answer": "After the free 30 minutes, charges apply:\n\n• 30–60 mins: +€0.50\n• 60–90 mins: +€1.50\n• 90–120 mins: +€3.50\n• Max charge: €4.00 per journey\n\n💡 Tip: Dock the bike before 30 minutes, wait 5 minutes, then take a new bike — the clock resets!"
                }
            ]
        },
        {
            "id": "stations",
            "label": "Finding Stations",
            "icon": "📍",
            "questions": [
                {
                    "id": "st_1",
                    "question": "How do I find the nearest station?",
                    "answer": "There are 115 stations across Dublin city centre — you're rarely more than a 5-minute walk from one! 🗺️\n\nBest ways to find one:\n• CityFlow Map tab — live map with colour-coded availability\n• dublinbikes.ie — official real-time map\n• Google Maps — search 'Dublin Bikes'\n\nGreen = bikes available, Red = empty, Yellow = low."
                },
                {
                    "id": "st_2",
                    "question": "What if a station is full and I can't return my bike?",
                    "answer": "Don't panic! Press the Return button at the full station terminal — you'll get an automatic 15-minute extension at no charge while you find another station nearby. 🔄\n\nBusy stations like Pearse Street and Grand Canal Dock fill up fast during rush hour — try nearby side streets!"
                }
            ]
        },
        {
            "id": "cycling_tips",
            "label": "Cycling in Dublin",
            "icon": "🌦️",
            "questions": [
                {
                    "id": "ct_1",
                    "question": "Is it safe to cycle in Dublin city centre?",
                    "answer": "Yes — Dublin's cycling infrastructure has improved a lot! Key safety tips:\n\n• Bring your own helmet (not provided) 🪖\n• Use the green cycle lanes where available\n• Cross Luas tram tracks at right angles\n• Be visible — use lights at night\n\nThe Grand Canal and Royal Canal towpaths are great car-free routes! 🌿"
                },
                {
                    "id": "ct_2",
                    "question": "What's the weather like for cycling in Dublin?",
                    "answer": "Dublin weather is famously unpredictable! ☁️\n\n• Spring (Mar–May): Cool, occasional showers — light jacket\n• Summer (Jun–Aug): Mild 15–20°C — best cycling weather!\n• Autumn (Sep–Nov): Wetter and windier — waterproof essential\n• Winter (Dec–Feb): Cold and wet — watch for ice\n\nCheck the CityFlow Weather tab for today's cycling score before heading out! 🌦️"
                }
            ]
        },
        {
            "id": "cityflow",
            "label": "About CityFlow",
            "icon": "📊",
            "questions": [
                {
                    "id": "cf_1",
                    "question": "What does CityFlow show me?",
                    "answer": "CityFlow is your all-in-one Dublin Bikes dashboard:\n\n• Live Map 📍 — real-time availability at every station, updated every 5 minutes\n• Weather 🌦️ — Dublin conditions with a cycling score\n• Station Search 🔍 — find any station by name or area\n• Availability Charts 📈 — historical patterns to plan your ride\n• This chatbot 🤖 — pre-written answers to common questions!"
                },
                {
                    "id": "cf_2",
                    "question": "How often is the CityFlow data updated?",
                    "answer": "CityFlow pulls live data from two sources:\n\n• JCDecaux Dublin Bikes API — bike and stand availability updates every 5 minutes ⚡\n• OpenWeatherMap API — weather updates every 10 minutes 🌤️\n\nHistorical data is stored in our database so you can see 24-hour trends for any station. Built for COMP30830 — UCD Dublin 2026 🎓"
                }
            ]
        }
    ]
}
 
 
@chat_bp.route("/api/chat/categories", methods=["GET"])
def get_categories():
    """Return all categories and questions (no answers — fetched on click)."""
    result = []
    for cat in QA["categories"]:
        result.append({
            "id":    cat["id"],
            "label": cat["label"],
            "icon":  cat["icon"],
            "questions": [
                {"id": q["id"], "question": q["question"]}
                for q in cat["questions"]
            ]
        })
    return jsonify(result)
 
 
@chat_bp.route("/api/chat/answer", methods=["POST"])
def get_answer():
    """Return the pre-written answer for a given question_id."""
    data = request.get_json(silent=True)
    if not data or "question_id" not in data:
        return jsonify({"error": "question_id is required"}), 400
 
    qid = data["question_id"]
    for cat in QA["categories"]:
        for q in cat["questions"]:
            if q["id"] == qid:
                return jsonify({
                    "question": q["question"],
                    "answer":   q["answer"],
                    "category": cat["label"]
                })
 
    return jsonify({"error": "Question not found"}), 404