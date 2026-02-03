class ClassifyService:
    """
    Rule-based classification service for support tickets.
    Analyzes ticket text to determine urgency, sentiment, and action requirements.
    """
    
    # Urgency classification keywords
    HIGH_URGENCY_KEYWORDS = {
        # Legal and compliance
        'lawsuit', 'legal action', 'attorney', 'lawyer', 'sue', 'court',
        'gdpr', 'data breach', 'privacy violation', 'regulation', 'compliance',
        
        # Financial critical
        'unauthorized charge', 'fraud', 'fraudulent', 'chargeback',
        'stolen', 'hacked', 'security breach', 'compromised account',
        
        # Service critical
        'production down', 'outage', 'critical', 'emergency', 'urgent',
        'immediately', 'asap', 'right now', 'data loss', 'cannot access',
        'complete failure', 'system down', 'not working at all'
    }
    
    MEDIUM_URGENCY_KEYWORDS = {
        # Financial issues
        'refund', 'overcharged', 'billing error', 'payment issue',
        'cancel subscription', 'wrong amount', 'duplicate charge',
        
        # Service issues
        'not working', 'broken', 'error', 'bug', 'issue', 'problem',
        'slow', 'timeout', 'performance', 'unavailable',
        
        # Account issues
        'locked out', 'reset password', 'access denied', 'login problem',
        'account suspended', 'missing data'
    }
    
    # Sentiment classification keywords
    NEGATIVE_SENTIMENT_KEYWORDS = {
        # Anger and frustration
        'angry', 'frustrated', 'furious', 'outraged', 'disgusted',
        'annoyed', 'irritated', 'upset', 'mad', 'rage',
        
        # Disappointment
        'disappointed', 'let down', 'terrible', 'awful', 'horrible',
        'worst', 'pathetic', 'useless', 'garbage', 'trash',
        
        # Complaints
        'unacceptable', 'ridiculous', 'incompetent', 'unprofessional',
        'poor service', 'bad experience', 'never again', 'lost customer',
        
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
        """
        # Combine subject and message for analysis (subject has more weight)
        text = f"{subject} {subject} {message}".lower()
        
        # Classify urgency
        urgency = ClassifyService._classify_urgency(text)
        
        # Classify sentiment
        sentiment = ClassifyService._classify_sentiment(text)
        
        # Determine if action is required
        requires_action = ClassifyService._classify_action_required(text)
        
        return {
            "urgency": urgency,
            "sentiment": sentiment,
            "requires_action": requires_action,
        }
    
    @staticmethod
    def _classify_urgency(text: str) -> str:
        """
        Determine urgency level based on keywords.
        High urgency takes precedence over medium, which takes precedence over low.
        Uses word boundary matching to avoid false positives.
        """
        import re
        
        # Check for high urgency keywords with word boundaries
        for keyword in ClassifyService.HIGH_URGENCY_KEYWORDS:
            # Use word boundary for single words, exact match for phrases
            if ' ' in keyword:
                # Multi-word phrase - use exact match
                if keyword in text:
                    return "high"
            else:
                # Single word - use word boundary
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    return "high"
        
        # Check for medium urgency keywords with word boundaries
        for keyword in ClassifyService.MEDIUM_URGENCY_KEYWORDS:
            if ' ' in keyword:
                if keyword in text:
                    return "medium"
            else:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    return "medium"
        
        # Default to low urgency
        return "low"
    
    @staticmethod
    def _classify_sentiment(text: str) -> str:
        """
        Determine sentiment based on keywords.
        Uses a scoring system where negative and positive keywords are counted.
        Uses word boundary matching to avoid false positives.
        """
        import re
        
        negative_count = 0
        positive_count = 0
        
        # Count negative keywords
        for keyword in ClassifyService.NEGATIVE_SENTIMENT_KEYWORDS:
            if ' ' in keyword or "'" in keyword:
                # Multi-word phrase or contraction - use exact match
                if keyword in text:
                    negative_count += 1
            else:
                # Single word - use word boundary
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
        # Negative sentiment requires at least one negative keyword
        # and more negative than positive keywords
        if negative_count > 0 and negative_count > positive_count:
            return "negative"
        elif positive_count > 0 and positive_count > negative_count:
            return "positive"
        else:
            return "neutral"
    
    @staticmethod
    def _classify_action_required(text: str) -> bool:
        """
        Determine if action is required based on keywords.
        Returns True if any action-related keywords are found.
        Uses word boundary matching to avoid false positives.
        """
        import re
        
        for keyword in ClassifyService.ACTION_REQUIRED_KEYWORDS:
            if ' ' in keyword:
                # Multi-word phrase - use exact match
                if keyword in text:
                    return True
            else:
                # Single word - use word boundary
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    return True
        
        return False
