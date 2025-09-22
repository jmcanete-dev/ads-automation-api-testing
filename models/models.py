from flask_sqlalchemy import SQLAlchemy
import pytz
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy import func, ForeignKey
from sqlalchemy.dialects.postgresql import JSON, BYTEA, ENUM, TIMESTAMP
from sqlalchemy.orm import validates
from datetime import datetime, timedelta

db = SQLAlchemy()

manila_tz = pytz.timezone("Asia/Manila")

class User(db.Model):
    __tablename__ = 'marketing_users'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(db.String(50), unique=True, nullable=False)
    username = db.Column(db.String(50), unique=True, nullable=False)
    full_name = db.Column(db.String(100))
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    gender = db.Column(ENUM('male', 'female', name='gender_enum'), nullable=False)
    userdomain = db.Column(db.String(100), nullable=False)
    profile_image = db.Column(BYTEA)
    user_status = db.Column(ENUM('active', 'inactive', name='status_enum'), default='active')
    user_level = db.Column(db.Integer, default=3)
    user_role = db.Column(db.String(50), default='staff')
    
    # Set timezone-aware timestamp columns
    created_at = db.Column(
        TIMESTAMP(timezone=True),
        default=lambda: datetime.now(manila_tz)
    )
    last_active = db.Column(
        TIMESTAMP(timezone=True),
        default=lambda: datetime.now(manila_tz),
        onupdate=lambda: datetime.now(manila_tz)
    )

class Campaign(db.Model):
    __tablename__ = 'campaign_table'

    campaign_id = db.Column(db.BigInteger, primary_key=True)  # Primary key without autoincrement
    user_id = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=False)  # Foreign key to user
    ad_account_id = db.Column(db.String(50), nullable=False)
    page_name = db.Column(db.String(255))
    sku = db.Column(db.String(50))
    material_code = db.Column(db.String(50))
    campaign_code = db.Column(db.String(50))
    daily_budget = db.Column(db.Float)
    facebook_page_id = db.Column(db.String(50))
    video_url = db.Column(db.String(255))
    headline = db.Column(db.String(255))
    primary_text = db.Column(db.Text)
    image_url = db.Column(db.String(255))
    product = db.Column(db.String(50))
    interests_list = db.Column(JSON, nullable=True)
    exclude_ph_regions = db.Column(JSON, nullable=True)
    adsets_ads_creatives = db.Column(JSON, nullable=True)
    is_ai = db.Column(db.Boolean, nullable=False, default=False)  # Indicates if AI generated the adsets
    is_ppe = db.Column(db.Boolean, nullable=False, default=False)
    access_token = db.Column(db.Text, nullable=False)
    status = db.Column(ENUM('Failed', 'Generating', 'Created', name='campaign_status_enum'), default='Generating')
    last_server_message = db.Column(db.Text)
    created_at = db.Column(db.TIMESTAMP, server_default=func.now())

class CampaignsScheduled(db.Model):
    __tablename__ = 'campaigns_scheduled'

    ad_account_id = db.Column(db.BigInteger, primary_key=True)  # Primary key as requested
    user_id = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=False)
    access_token = db.Column(db.Text, nullable=False)
    schedule_data = db.Column(MutableDict.as_mutable(JSON), nullable=False)
    campaign_code = db.Column(db.String(255), nullable=True)
    added_at = db.Column(TIMESTAMP, server_default=func.now(), nullable=False)
    matched_campaign_data = db.Column(MutableDict.as_mutable(JSON), nullable=True)
    last_time_checked = db.Column(TIMESTAMP, nullable=True, default=datetime.utcnow)
    last_check_status = db.Column(ENUM('Failed', 'Success', 'Ongoing', name='check_status_enum'), nullable=False, default='Success')  # Status for last check
    last_check_message = db.Column(db.Text, nullable=True)   # Tracks the last time campaigns were checked
    task_id = db.Column(db.String(255), nullable=True)

    @validates('schedule_data')
    def validate_schedule_data(self, key, value):
        # Check if campaign_code exists in the schedule_data
        if isinstance(value, list) and len(value) > 0:
            schedule = value[0]
            if 'campaign_code' in schedule:
                self.campaign_code = schedule['campaign_code']  # Save campaign_code in its column
        return value

class CampaignOffOnly(db.Model):
    __tablename__ = 'campaign_off_only'

    ad_account_id = db.Column(db.String(50), primary_key=True)  # Primary key
    user_id = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=False)
    access_token = db.Column(db.Text, nullable=False)
    schedule_data = db.Column(MutableDict.as_mutable(JSON), nullable=False)
    campaigns_data = db.Column(MutableDict.as_mutable(JSON), nullable=True)  # Single field for campaigns data
    added_at = db.Column(TIMESTAMP, server_default=func.now(), nullable=False)
    last_time_checked = db.Column(TIMESTAMP, nullable=True, default=datetime.utcnow)
    last_check_status = db.Column(ENUM('Failed', 'Success', 'Ongoing', name='campaign_off_status_enum'),
                                  nullable=False, default='Success')  # Updated ENUM name
    last_check_message = db.Column(db.Text, nullable=True)  # Tracks last check status message
    task_id = db.Column(db.String(255), nullable=True)  # Celery task tracking

class PHRegionTable(db.Model):
    __tablename__ = "ph_region_tables"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    region_name = db.Column(db.String(100), nullable=False)
    region_key = db.Column(db.Integer, unique=True, nullable=False)
    country_code = db.Column(db.String(10), nullable=False, default="PH")

class PHCityTable(db.Model):
    __tablename__ = "ph_city_tables"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    city_name = db.Column(db.String(100), nullable=False)
    city_key = db.Column(db.BigInteger, unique=True, nullable=False)
    country_code = db.Column(db.String(10), nullable=False, default="PH")

class CampaignCode(db.Model):
    __tablename__ = 'tbl_campaign_code'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=False)  # Foreign key to marketing_users table
    campaign_code = db.Column(db.String(255), nullable=False)  # Campaign code with a maximum length of 5 characters

    user = db.relationship('User', backref=db.backref('campaign_codes', lazy=True))  # Relationship with User model

class AccessToken(db.Model):
    __tablename__ = 'access_tokens'
    
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=False)
    access_token = db.Column(db.String(255), unique=True, nullable=False)
    facebook_name = db.Column(db.String(100))
    is_expire = db.Column(db.Boolean, default=False)
    expiring_at = db.Column(TIMESTAMP(timezone=True), nullable=False)
    
    # Metadata
    created_at = db.Column(
        TIMESTAMP(timezone=True),
        default=lambda: datetime.now(manila_tz)
    )
    last_used_at = db.Column(
        TIMESTAMP(timezone=True),
        default=lambda: datetime.now(manila_tz),
        onupdate=lambda: datetime.now(manila_tz)
    )
    
    # Relationship with User model
    user = db.relationship('User', backref=db.backref('access_tokens', lazy=True))
    
    @validates('access_token')
    def validate_token(self, key, access_token):
        if not access_token or len(access_token) < 32:
            raise ValueError("Access token must be at least 32 characters long")
        return access_token

    @classmethod
    def get_superadmin_tokens_for_client(cls, client_id):
        """
        Get access tokens from the client's managing superadmin
        Example: If client ID 2 is managed by superadmin ID 1, this will return all tokens belonging to superadmin ID 1
        """
        # Get the relationship with superadmin
        relationship = UserRelationship.query.filter_by(
            client_id=client_id,
            is_active=True
        ).first()
        
        if not relationship:
            return []
            
        # Get all tokens from the managing superadmin
        return cls.query.filter_by(user_id=relationship.superadmin_id).all()

    @classmethod
    def get_client_accessible_tokens(cls, client_id):
        """
        Get all tokens that a client can access (only from their managing superadmin)
        Example: If client ID 2 is managed by superadmin ID 1, this will return all tokens belonging to superadmin ID 1
        """
        return cls.get_superadmin_tokens_for_client(client_id)

class UserRelationship(db.Model):
    __tablename__ = 'user_relationships'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    superadmin_id = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=False)
    client_id = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    
    # Metadata
    created_at = db.Column(
        TIMESTAMP(timezone=True),
        default=lambda: datetime.now(manila_tz)
    )
    updated_at = db.Column(
        TIMESTAMP(timezone=True),
        default=lambda: datetime.now(manila_tz),
        onupdate=lambda: datetime.now(manila_tz)
    )

    # Relationships
    superadmin = db.relationship('User', 
                               foreign_keys=[superadmin_id],
                               backref=db.backref('managed_clients', lazy=True))
    client = db.relationship('User',
                           foreign_keys=[client_id],
                           backref=db.backref('managing_superadmin', lazy=True))

    @validates('superadmin_id', 'client_id')
    def validate_user_roles(self, key, user_id):
        user = User.query.get(user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} does not exist")
        
        if key == 'superadmin_id':
            if user.user_level != 1 or user.user_role != 'superadmin':
                raise ValueError("Superadmin must have user_level 1 and user_role 'superadmin'")
        elif key == 'client_id':
            if user.user_level == 1 or user.user_level == 2:  # superadmin or admin
                raise ValueError("Client cannot be a superadmin or admin")
            if user.user_level == 3 and user.user_role != 'staff':
                raise ValueError("Level 3 users must have role 'staff'")
            if user.user_level == 4 and user.user_role != 'client':
                raise ValueError("Level 4 users must have role 'client'")
        
        return user_id

    __table_args__ = (
        db.UniqueConstraint('superadmin_id', 'client_id', name='unique_superadmin_client'),
    )

class InviteCode(db.Model):
    __tablename__ = 'invite_codes'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    superadmin_id = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=False)
    invite_code = db.Column(db.String(10), unique=True, nullable=False)  # Short, unique code
    is_used = db.Column(db.Boolean, default=False)
    used_by = db.Column(db.BigInteger, ForeignKey('marketing_users.id'), nullable=True)
    
    # Metadata
    created_at = db.Column(
        TIMESTAMP(timezone=True),
        default=lambda: datetime.now(manila_tz)
    )
    used_at = db.Column(
        TIMESTAMP(timezone=True),
        nullable=True
    )
    expires_at = db.Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(manila_tz) + timedelta(days=7)  # Codes expire in 7 days
    )

    # Relationships
    superadmin = db.relationship('User', 
                               foreign_keys=[superadmin_id],
                               backref=db.backref('generated_invites', lazy=True))
    used_by_user = db.relationship('User',
                                 foreign_keys=[used_by],
                                 backref=db.backref('used_invite', lazy=True))

    @validates('superadmin_id')
    def validate_superadmin(self, key, superadmin_id):
        user = User.query.get(superadmin_id)
        if not user:
            raise ValueError(f"User with ID {superadmin_id} does not exist")
        if user.user_level != 1 or user.user_role != 'superadmin':
            raise ValueError("Invite code can only be generated by a level 1 superadmin")
        return superadmin_id

    @validates('used_by')
    def validate_client(self, key, used_by):
        if used_by:
            user = User.query.get(used_by)
            if not user:
                raise ValueError(f"User with ID {used_by} does not exist")
            if user.user_level == 1 or user.user_level == 2:  # superadmin or admin
                raise ValueError("Superadmins and admins cannot use invite codes")
            if user.user_level == 3 and user.user_role != 'staff':
                raise ValueError("Level 3 users must have role 'staff'")
            if user.user_level == 4 and user.user_role != 'client':
                raise ValueError("Level 4 users must have role 'client'")
        return used_by

class AdAccount(db.Model):
    __tablename__ = 'ad_accounts'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.String(255), nullable=False)  # Store the Facebook account_id
    name = db.Column(db.String(255))
    user_id = db.Column(db.Integer, nullable=False)  # Link to user
    created_at = db.Column(db.DateTime, default=datetime.now(manila_tz))
    updated_at = db.Column(db.DateTime, default=datetime.now(manila_tz), onupdate=datetime.now(manila_tz))
