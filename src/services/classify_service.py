class ClassifyService:
    """
    Rule-based classification service for support tickets.
    Analyzes ticket text to determine urgency, sentiment, and action requirements.
    
    Classification Rules:
    1. CRITICAL tickets (legal, chargeback, fraud) → HIGH urgency + NEGATIVE sentiment + requires_action
    2. Escalation phrases ("demand refund", "filing complaint") → elevate urgency
    3. Financial threats always require action
    4. Sentiment is overridden to negative for critical issues
    """
    
    # ========================================================================
    # CRITICAL KEYWORDS - These ALWAYS trigger HIGH urgency + require action
    # ========================================================================
    CRITICAL_KEYWORDS = {
        # Legal threats - immediate escalation required
        'lawsuit', 'legal action', 'attorney', 'lawyer', 'sue', 'court',
        'legal team', 'legal department', 'my lawyer', 'hear from my lawyer',
        'file a complaint', 'filing complaint', 'report you', 'bbb complaint',
        'consumer protection', 'ftc complaint', 'regulatory', 'authorities',
        
        # Chargeback/Fraud - financial risk
        'chargeback', 'charge back', 'dispute charge', 'disputing charge',
        'credit card dispute', 'bank dispute', 'reversing payment',
        'fraud', 'fraudulent', 'unauthorized charge', 'unauthorized transaction',
        'stolen card', 'identity theft', 'scam', 'scammed',
        
        # Data/Security breach - compliance risk
        'data breach', 'security breach', 'privacy violation', 'gdpr',
        'data leak', 'leaked data', 'compromised', 'hacked', 'stolen data',
        
        # Regulatory/Compliance
        'compliance violation', 'hipaa', 'pci', 'sox', 'ccpa',
    }
    
    # ========================================================================
    # ESCALATION PHRASES - Upgrade medium issues to high urgency
    # ========================================================================
    ESCALATION_PHRASES = {
        # Demand language
        'demand refund', 'demand my money', 'want my money back now',
        'full refund now', 'immediate refund', 'refund immediately',
        'give me my money', 'return my money',
        
        # Threat language
        'cancel everything', 'closing my account', 'never use again',
        'telling everyone', 'social media', 'going public', 'warn others',
        'review everywhere', 'bad review', 'negative review',
        
        # Time pressure
        'last chance', 'final warning', 'one more day', 'today or else',
        'running out of patience', 'patience is gone', 'had enough',
    }
    
    # Urgency classification keywords (non-critical)
    HIGH_URGENCY_KEYWORDS = {
        # Service critical
        'production down', 'outage', 'critical', 'emergency', 'urgent',
        'immediately', 'asap', 'right now', 'data loss', 'cannot access',
        'complete failure', 'system down', 'not working at all',
        'all users affected', 'business stopped', 'losing money',
    }
    
    MEDIUM_URGENCY_KEYWORDS = {
        # Financial issues (non-critical)
        'refund', 'overcharged', 'billing error', 'payment issue',
        'cancel subscription', 'wrong amount', 'duplicate charge',
        'charged twice', 'double charged',
        
        # Service issues
        'not working', 'broken', 'error', 'bug', 'issue', 'problem',
        'slow', 'timeout', 'performance', 'unavailable',
        
        # Account issues
        'locked out', 'reset password', 'access denied', 'login problem',
        'account suspended', 'missing data', 'cannot login', 'login issue'
    }
    
    # Sentiment classification keywords
    NEGATIVE_SENTIMENT_KEYWORDS = {
        # Anger and frustration
        'angry', 'frustrated', 'furious', 'outraged', 'disgusted',
        'annoyed', 'irritated', 'upset', 'mad', 'rage', 'livid',
        'infuriated', 'seething', 'fuming',
        
        # Disappointment
        'disappointed', 'let down', 'terrible', 'awful', 'horrible',
        'worst', 'pathetic', 'useless', 'garbage', 'trash', 'joke',
        'disaster', 'nightmare', 'unbelievable',
        
        # Complaints
        'unacceptable', 'ridiculous', 'incompetent', 'unprofessional',
        'poor service', 'bad experience', 'never again', 'lost customer',
        'waste of time', 'waste of money', 'rip off', 'ripoff',
        
        # Problems
        'broken', 'failed', 'doesn\'t work', 'not working', 'defective',
        'damaged', 'wrong', 'incorrect', 'missing', 'lost'
    }
    
    POSITIVE_SENTIMENT_KEYWORDS = {
        'thank', 'thanks', 'appreciate', 'grateful', 'excellent',
        'great', 'good', 'awesome', 'amazing', 'wonderful',
        'helpful', 'perfect', 'love', 'fantastic', 'outstanding',
        'satisfied', 'happy', 'pleased', 'impressed', 'brilliant'
    }
    
    # Action requirement keywords
    ACTION_REQUIRED_KEYWORDS = {
        # Requests
        'please', 'need', 'want', 'require', 'request', 'can you',
        'could you', 'would you', 'help me', 'assist', 'support',
        
        # Problems requiring resolution
        'fix', 'resolve', 'solve', 'repair', 'correct', 'address',
        'investigate', 'look into', 'check', 'review',
        
        # Account actions
        'refund', 'cancel', 'change', 'update', 'modify', 'reset',
        'delete', 'remove', 'add', 'upgrade', 'downgrade',
        
        # Information requests
        'explain', 'clarify', 'tell me', 'show me', 'how do i',
        'when will', 'why is', 'what is', 'where is'
    }
    
    @staticmethod
    def classify(message: str, subject: str) -> dict:
        """
        Classify a ticket based on its message and subject.
        
        Args:
            message: The ticket message body
            subject: The ticket subject line
            
        Returns:
            Dictionary with urgency, sentiment, and requires_action fields
            
        Consistency Rules:
        1. CRITICAL issues → HIGH urgency + NEGATIVE sentiment + requires_action=True
        2. Escalation phrases can elevate medium → high urgency
        3. High urgency financial/legal issues always require action
        """
        # Combine subject and message for analysis (subject has more weight)
        text = f"{subject} {subject} {message}".lower()
        
        # First check for CRITICAL keywords (legal, fraud, chargeback)
        is_critical = ClassifyService._is_critical(text)
        
        # Check for escalation phrases
        has_escalation = ClassifyService._has_escalation(text)
        
        # Classify urgency
        urgency = ClassifyService._classify_urgency(text, is_critical, has_escalation)
        
        # Classify sentiment (may be overridden for critical issues)
        sentiment = ClassifyService._classify_sentiment(text, is_critical)
        
        # Determine if action is required (critical issues always require action)
        requires_action = ClassifyService._classify_action_required(text, is_critical, urgency)
        
        return {
            "urgency": urgency,
            "sentiment": sentiment,
            "requires_action": requires_action,
        }
    
    @staticmethod
    def _is_critical(text: str) -> bool:
        """Check if ticket contains critical keywords (legal, fraud, chargeback)."""
        import re
        
        for keyword in ClassifyService.CRITICAL_KEYWORDS:
            if ' ' in keyword:
                if keyword in text:
                    return True
            else:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    return True
        return False
    
    @staticmethod
    def _has_escalation(text: str) -> bool:
        """Check if ticket contains escalation phrases."""
        for phrase in ClassifyService.ESCALATION_PHRASES:
            if phrase in text:
                return True
        return False
    
    @staticmethod
    def _classify_urgency(text: str, is_critical: bool, has_escalation: bool) -> str:
        """
        Determine urgency level based on keywords and context.
        
        Priority order:
        1. Critical keywords → always HIGH
        2. Escalation phrases → elevate to HIGH
        3. High urgency keywords → HIGH
        4. Medium urgency keywords → MEDIUM
        5. Default → LOW
        """
        import re
        
        # Critical issues are always high urgency
        if is_critical:
            return "high"
        
        # Check for high urgency keywords with word boundaries
        for keyword in ClassifyService.HIGH_URGENCY_KEYWORDS:
            if ' ' in keyword:
                if keyword in text:
                    return "high"
            else:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    return "high"
        
        # Check for medium urgency keywords
        is_medium = False
        for keyword in ClassifyService.MEDIUM_URGENCY_KEYWORDS:
            if ' ' in keyword:
                if keyword in text:
                    is_medium = True
                    break
            else:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    is_medium = True
                    break
        
        # Escalation phrases elevate medium → high
        if is_medium and has_escalation:
            return "high"
        
        if is_medium:
            return "medium"
        
        # Default to low urgency
        return "low"
    
    @staticmethod
    def _classify_sentiment(text: str, is_critical: bool = False) -> str:
        """
        Determine sentiment based on keywords.
        
        Consistency Rules:
        - Critical issues (legal, fraud, chargeback) are ALWAYS negative sentiment
        - Otherwise, uses scoring system with negative/positive keyword counts
        """
        import re
        
        # Critical issues are always negative - customer threatening legal action,
        # reporting fraud, or filing chargeback is inherently a negative situation
        if is_critical:
            return "negative"
        
        negative_count = 0
        positive_count = 0
        
        # Count negative keywords
        for keyword in ClassifyService.NEGATIVE_SENTIMENT_KEYWORDS:
            if ' ' in keyword or "'" in keyword:
                if keyword in text:
                    negative_count += 1
            else:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    negative_count += 1
        
        # Count positive keywords
        for keyword in ClassifyService.POSITIVE_SENTIMENT_KEYWORDS:
            if ' ' in keyword:
                if keyword in text:
                    positive_count += 1
            else:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    positive_count += 1
        
        # Determine sentiment based on counts
        if negative_count > 0 and negative_count > positive_count:
            return "negative"
        elif positive_count > 0 and positive_count > negative_count:
            return "positive"
        else:
            return "neutral"
    
    @staticmethod
    def _classify_action_required(text: str, is_critical: bool = False, urgency: str = "low") -> bool:
        """
        Determine if action is required based on keywords and context.
        
        Consistency Rules:
        - Critical issues ALWAYS require action (legal, fraud, chargeback)
        - High urgency issues ALWAYS require action
        - Otherwise, checks for action-related keywords
        """
        import re
        
        # Critical issues always require immediate action
        if is_critical:
            return True
        
        # High urgency issues always require action
        if urgency == "high":
            return True
        
        # Check for action keywords
        for keyword in ClassifyService.ACTION_REQUIRED_KEYWORDS:
            if ' ' in keyword:
                if keyword in text:
                    return True
            else:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    return True
        
        return False
        
        return False
