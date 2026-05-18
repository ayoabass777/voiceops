"""
Transcript templates for VoiceOps call simulation.

Each template is a realistic phone conversation for one of the three verticals.
Templates include a mix of intents, sentiments, and resolution outcomes.
"""

PLUMBING_TRANSCRIPTS = [
    {
        "transcript": (
            "Agent: Thank you for calling Cascade Plumbing, how can I help you today?\n"
            "Customer: Hi, yeah, my kitchen sink has been leaking under the cabinet for about two days now. "
            "It's getting worse and there's water damage on the floor.\n"
            "Agent: I'm sorry to hear that. Let me get you scheduled for a service visit. "
            "Are you available tomorrow morning between 8 and 10?\n"
            "Customer: Tomorrow morning works. Can you give me a rough estimate on cost?\n"
            "Agent: For a standard leak repair, it typically runs between 150 and 300 dollars "
            "depending on the issue. The technician will give you an exact quote on site.\n"
            "Customer: That sounds reasonable. Let's book it.\n"
            "Agent: Perfect, you're confirmed for tomorrow between 8 and 10 AM. "
            "The technician will call 30 minutes before arrival. Is there anything else?\n"
            "Customer: No, that's it. Thanks.\n"
            "Agent: Have a great day!"
        ),
        "duration_range": (120, 180),
        "expected_intent": "booking",
        "expected_sentiment": 3.8,
    },
    {
        "transcript": (
            "Agent: Good afternoon, Westside Drain Co, this is the after-hours line.\n"
            "Customer: I need someone out here right now. My basement is flooding. "
            "There's sewage backing up through the floor drain and it smells terrible.\n"
            "Agent: I understand this is urgent. I'm dispatching our emergency team immediately. "
            "Can I get your address?\n"
            "Customer: 4521 Oak Street. Please hurry, the water is rising.\n"
            "Agent: Our team will be there within 45 minutes. In the meantime, "
            "if you can safely reach the main water shutoff valve, please turn it off.\n"
            "Customer: Okay, I'll try. Just get someone here fast.\n"
            "Agent: They're on their way. You'll get a text with the technician's ETA shortly."
        ),
        "duration_range": (60, 120),
        "expected_intent": "emergency",
        "expected_sentiment": 1.8,
    },
    {
        "transcript": (
            "Agent: Peak Plumbing and Heating, how can I help?\n"
            "Customer: I'm calling about the repair your guy did last week on my hot water tank. "
            "It's still not working right. The water gets lukewarm at best.\n"
            "Agent: I apologize for the inconvenience. Let me pull up your service record. "
            "Can I have your name?\n"
            "Customer: Dave Morrison. And honestly, I'm pretty frustrated. "
            "I paid 400 dollars and it's the same problem.\n"
            "Agent: I completely understand your frustration, Mr. Morrison. "
            "I can see the notes from last week. I'm going to send our senior technician out "
            "at no additional charge to take another look. Would Thursday work?\n"
            "Customer: I guess Thursday is fine. But if it's not fixed this time, "
            "I want a full refund.\n"
            "Agent: That's absolutely fair. Let me note that on your file. "
            "Thursday between 1 and 3 PM. You'll hear from us before then."
        ),
        "duration_range": (180, 300),
        "expected_intent": "complaint",
        "expected_sentiment": 2.0,
    },
    {
        "transcript": (
            "Agent: Cascade Plumbing, how can I assist you?\n"
            "Customer: Hi, I'm renovating my bathroom and I need to get a quote "
            "for moving some plumbing around. New vanity location, relocating the toilet, "
            "and adding a second shower head.\n"
            "Agent: Sure, we do renovation plumbing. For a job that size, "
            "we'd want to send someone out to assess the space first. "
            "The assessment visit is free. When works for you?\n"
            "Customer: Could someone come Saturday?\n"
            "Agent: We have a Saturday slot open at 11 AM. I'll book that for you.\n"
            "Customer: Great. Do you work with contractors too? "
            "My contractor wants to coordinate on the timeline.\n"
            "Agent: Absolutely, we coordinate with contractors all the time. "
            "Just have them call us after the assessment and we'll align schedules.\n"
            "Customer: Perfect, thanks so much."
        ),
        "duration_range": (150, 240),
        "expected_intent": "inquiry",
        "expected_sentiment": 4.2,
    },
    {
        "transcript": (
            "Agent: Thank you for calling Westside Drain Co.\n"
            "Customer: Hi, I had an appointment booked for this Friday "
            "for a drain cleaning but I need to cancel it.\n"
            "Agent: No problem at all. Can I have your name?\n"
            "Customer: Sarah Chen.\n"
            "Agent: Got it, Sarah. Your Friday appointment has been cancelled. "
            "Would you like to reschedule for a later date?\n"
            "Customer: Not right now, I'll call back when I know my schedule better.\n"
            "Agent: Sounds good. We're here whenever you're ready. Have a great day."
        ),
        "duration_range": (45, 90),
        "expected_intent": "cancellation",
        "expected_sentiment": 3.5,
    },
]

AUTO_REPAIR_TRANSCRIPTS = [
    {
        "transcript": (
            "Agent: Metro Auto Care, this is Jason. How can I help?\n"
            "Customer: Hi, I need to book an oil change and tire rotation for my 2019 Honda Civic.\n"
            "Agent: Sure thing. We have availability this Thursday or Friday. Preference?\n"
            "Customer: Thursday morning if possible.\n"
            "Agent: I can do 9 AM Thursday. Oil change and tire rotation together "
            "runs about 89 dollars plus tax.\n"
            "Customer: That's fine. Book me in.\n"
            "Agent: You're all set for Thursday at 9. Plan for about an hour. "
            "We'll text you when it's ready.\n"
            "Customer: Thanks, Jason."
        ),
        "duration_range": (60, 120),
        "expected_intent": "booking",
        "expected_sentiment": 4.0,
    },
    {
        "transcript": (
            "Agent: AutoFix Pro, how can I help you today?\n"
            "Customer: Yeah, I picked up my car yesterday after the brake job "
            "and there's a grinding noise when I turn left. It wasn't doing that before.\n"
            "Agent: That's definitely something we want to look at right away. "
            "Can you bring it back in today?\n"
            "Customer: I'm at work until 5. Can I come after?\n"
            "Agent: We close at 6, but I'll make sure someone stays to check it. "
            "Come straight here after work. If it's related to the brake work, "
            "there's no charge.\n"
            "Customer: It better be related because I just paid 600 bucks. "
            "I shouldn't be hearing new noises.\n"
            "Agent: I understand. We'll take care of it. See you after 5."
        ),
        "duration_range": (120, 180),
        "expected_intent": "complaint",
        "expected_sentiment": 2.2,
    },
    {
        "transcript": (
            "Agent: Metro Auto Care.\n"
            "Customer: Hi, I'm calling to check on the status of my car. "
            "I dropped it off Monday for the transmission diagnostic.\n"
            "Agent: Let me check. What's your last name?\n"
            "Customer: Patel.\n"
            "Agent: Okay, Mr. Patel. The diagnostic is done. "
            "Our tech found the issue — it's the torque converter. "
            "The repair estimate is around 1,800 dollars.\n"
            "Customer: Eighteen hundred? That's steep. "
            "Can I think about it and call you back?\n"
            "Agent: Of course. We'll hold the car for up to a week with no storage fee. "
            "If you want a second opinion, that's totally fine too.\n"
            "Customer: Okay, I'll call back tomorrow. Thanks."
        ),
        "duration_range": (120, 180),
        "expected_intent": "follow_up",
        "expected_sentiment": 2.8,
    },
    {
        "transcript": (
            "Agent: Precision Auto Works, good morning.\n"
            "Customer: Morning! I just moved to the area and I'm looking for "
            "a reliable shop for my truck. Do you guys work on Ford F-150s?\n"
            "Agent: Absolutely, we service all domestic and import trucks. "
            "What year is your F-150?\n"
            "Customer: 2021. It's due for its 60,000 mile service.\n"
            "Agent: We can handle that. The 60K service includes fluid changes, "
            "filter replacements, brake inspection, and a full diagnostic. "
            "Usually runs about 450 to 550 depending on what we find.\n"
            "Customer: That's in line with what I've been paying. Can I book for next week?\n"
            "Agent: How about Tuesday at 8 AM? Plan to leave it for the day.\n"
            "Customer: Tuesday works. Thanks!"
        ),
        "duration_range": (120, 180),
        "expected_intent": "booking",
        "expected_sentiment": 4.5,
    },
    {
        "transcript": (
            "Agent: AutoFix Pro, how can I help?\n"
            "Customer: Do you guys do pre-purchase inspections? "
            "I'm buying a used car from a private seller and I want it checked out first.\n"
            "Agent: We do. It's a 150-dollar flat rate for a full bumper-to-bumper inspection "
            "with a written report. Takes about two hours.\n"
            "Customer: Can the seller bring it in, or do I need to be there?\n"
            "Agent: Either way works. We just need someone to authorize the inspection.\n"
            "Customer: Good to know. I'll call back once I schedule with the seller.\n"
            "Agent: Sounds good. We usually have same-week availability."
        ),
        "duration_range": (90, 150),
        "expected_intent": "inquiry",
        "expected_sentiment": 4.0,
    },
]

PROPERTY_MANAGEMENT_TRANSCRIPTS = [
    {
        "transcript": (
            "Agent: Apex Property Group, tenant services. How can I help?\n"
            "Customer: Hi, this is unit 4B at the Riverside complex. "
            "There's water leaking from the ceiling in my bathroom. "
            "It looks like it's coming from the unit above me.\n"
            "Agent: I'm sorry about that. Is the leak active right now?\n"
            "Customer: Yes, it's a steady drip. I put a bucket under it.\n"
            "Agent: Good thinking. I'm logging this as urgent and notifying maintenance. "
            "Someone will be out within two hours. I'll also reach out to the unit above "
            "to check their plumbing.\n"
            "Customer: Thank you. Should I do anything else in the meantime?\n"
            "Agent: Just keep the bucket there and avoid using the bathroom if possible. "
            "We'll call you when the tech is on the way."
        ),
        "duration_range": (120, 180),
        "expected_intent": "emergency",
        "expected_sentiment": 2.5,
    },
    {
        "transcript": (
            "Agent: Harbour Living Management, how can I assist you?\n"
            "Customer: My lease is up in two months and I'm thinking about renewing. "
            "Is there going to be a rent increase?\n"
            "Agent: Let me pull up your file. What unit are you in?\n"
            "Customer: Unit 312, the name's Kim.\n"
            "Agent: Hi Kim. So for your renewal, there is a 3 percent increase "
            "which brings your monthly rent from 1,950 to 2,008 dollars.\n"
            "Customer: That's not bad actually. Can I get a two-year lease "
            "to lock that rate in?\n"
            "Agent: I'd need to check with the property owner on a two-year term. "
            "I'll have an answer for you by Friday. Does that work?\n"
            "Customer: Yeah, that's fine. Just email me.\n"
            "Agent: Will do. Thanks for calling, Kim."
        ),
        "duration_range": (120, 210),
        "expected_intent": "inquiry",
        "expected_sentiment": 3.8,
    },
    {
        "transcript": (
            "Agent: Apex Property Group, how can I help?\n"
            "Customer: This is the third time I'm calling about the broken dishwasher "
            "in unit 7A. I submitted a maintenance request two weeks ago. "
            "Nobody has come.\n"
            "Agent: I sincerely apologize. Let me look into this right now.\n"
            "Customer: Every time I call, someone says they'll look into it. "
            "I'm paying 2,200 a month and I can't even wash my dishes.\n"
            "Agent: You're right, and that's unacceptable. I'm escalating this directly "
            "to our maintenance supervisor. I'm going to give you his direct line "
            "so you can follow up if needed. Can I also offer a credit "
            "on next month's rent for the inconvenience?\n"
            "Customer: A credit would be nice, yeah. But I mainly just want the dishwasher fixed.\n"
            "Agent: Understood. You'll hear from the supervisor by end of day today. "
            "I'm putting a note that this is priority one."
        ),
        "duration_range": (180, 300),
        "expected_intent": "complaint",
        "expected_sentiment": 1.5,
    },
    {
        "transcript": (
            "Agent: Harbour Living Management.\n"
            "Customer: Hi, I'm moving out at the end of the month. "
            "What do I need to do for the move-out inspection?\n"
            "Agent: We'll schedule an inspection for the last day of your lease "
            "or the day after. You'll want to make sure the unit is clean, "
            "all personal items are removed, and any minor damage is patched.\n"
            "Customer: What about the wall anchors I put in? Do I need to remove those?\n"
            "Agent: Yes, remove them and fill the holes with white spackle. "
            "Small nail holes are fine, but anything bigger than a quarter "
            "should be patched.\n"
            "Customer: Got it. And my deposit?\n"
            "Agent: Your deposit will be returned within 15 business days "
            "after the inspection, minus any deductions. "
            "We'll send you an itemized statement.\n"
            "Customer: Okay, thanks for the info."
        ),
        "duration_range": (120, 180),
        "expected_intent": "inquiry",
        "expected_sentiment": 3.5,
    },
    {
        "transcript": (
            "Agent: Apex Property Group, tenant services.\n"
            "Customer: Hi, I was supposed to have a maintenance visit today "
            "between 10 and 12 for my thermostat. Nobody showed up.\n"
            "Agent: Let me check the schedule. What unit?\n"
            "Customer: 9C.\n"
            "Agent: I see the appointment. It looks like the tech got pulled "
            "to an emergency call. I apologize that nobody contacted you.\n"
            "Customer: That's really frustrating. I took the morning off work for this.\n"
            "Agent: I completely understand. Let me reschedule you for first thing "
            "tomorrow morning, 8 AM sharp. I'll make sure you're the first stop. "
            "And I'll have the maintenance manager call you to confirm.\n"
            "Customer: Fine. But if they don't show up again, I'm filing a complaint "
            "with the tenancy board.\n"
            "Agent: I hear you. It won't happen again. You have my word."
        ),
        "duration_range": (150, 240),
        "expected_intent": "complaint",
        "expected_sentiment": 1.8,
    },
]


# Map business_id to vertical templates
BUSINESS_TEMPLATES = {
    "a1b2c3d4-1111-4000-8000-000000000001": PLUMBING_TRANSCRIPTS,            # Cascade Plumbing
    "a1b2c3d4-2222-4000-8000-000000000002": PLUMBING_TRANSCRIPTS,            # Westside Drain Co
    "a1b2c3d4-3333-4000-8000-000000000003": AUTO_REPAIR_TRANSCRIPTS,         # Metro Auto Care
    "a1b2c3d4-4444-4000-8000-000000000004": AUTO_REPAIR_TRANSCRIPTS,         # AutoFix Pro
    "a1b2c3d4-5555-4000-8000-000000000005": PROPERTY_MANAGEMENT_TRANSCRIPTS, # Apex Property Group
    "a1b2c3d4-6666-4000-8000-000000000006": PROPERTY_MANAGEMENT_TRANSCRIPTS, # Harbour Living Mgmt
    "a1b2c3d4-7777-4000-8000-000000000007": PLUMBING_TRANSCRIPTS,            # Peak Plumbing & Heating
    "a1b2c3d4-8888-4000-8000-000000000008": AUTO_REPAIR_TRANSCRIPTS,         # Precision Auto Works
}
