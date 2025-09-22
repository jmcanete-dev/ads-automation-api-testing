from flask import jsonify
from models.models import User, UserRelationship, manila_tz, db
from datetime import datetime

def get_relationships(superadmin_id):
    try:
        # Get the user object
        user = User.query.get(superadmin_id)
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        # Only superadmins can view relationships
        if user.user_level != 1 or user.user_role != 'superadmin':
            return jsonify({'error': 'Unauthorized access'}), 403
            
        # Get all relationships where this user is the superadmin
        # No longer filtering by is_active since we're doing hard deletes
        relationships = UserRelationship.query.filter_by(
            superadmin_id=superadmin_id
        ).all()
        
        # Format the response data
        relationships_data = []
        for rel in relationships:
            client = User.query.get(rel.client_id)
            if client:
                relationships_data.append({
                    'id': rel.id,
                    'client_id': rel.client_id,
                    'client_name': client.full_name,
                    'client_email': client.email,
                    'client_role': client.user_role,
                    'created_at': rel.created_at.isoformat(),
                    'updated_at': rel.updated_at.isoformat()
                })
        
        return jsonify({
            'message': 'Relationships retrieved successfully',
            'data': relationships_data
        }), 200
        
    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'error': 'Internal Server Error'}), 500

def delete_relationship(relationship_id, superadmin_id):
    try:
        # Get the user object
        user = User.query.get(superadmin_id)
        if not user:
            return jsonify({'error': 'User not found'}), 404
            
        # Only superadmins can delete relationships
        if user.user_level != 1 or user.user_role != 'superadmin':
            return jsonify({'error': 'Unauthorized access'}), 403
            
        # Find the relationship
        relationship = UserRelationship.query.get(relationship_id)
        if not relationship:
            return jsonify({'error': 'Relationship not found'}), 404
            
        # Verify the relationship belongs to this superadmin
        if relationship.superadmin_id != superadmin_id:
            return jsonify({'error': 'Unauthorized access'}), 403
            
        # Hard delete the relationship record
        db.session.delete(relationship)
        db.session.commit()
        
        return jsonify({
            'message': 'Relationship deleted successfully'
        }), 200
        
    except Exception as e:
        print("[EXCEPTION]", str(e))
        db.session.rollback()
        return jsonify({'error': 'Internal Server Error'}), 500
        
    except Exception as e:
        print("[EXCEPTION]", str(e))
        db.session.rollback()
        return jsonify({'error': 'Internal Server Error'}), 500

def check_relationship(user_id):
    try:
        # Find the relationship for this user
        relationship = UserRelationship.query.filter_by(
            client_id=int(user_id),  # Convert to int for database query
            is_active=True
        ).first()

        if relationship:
            # Get the superadmin's name
            superadmin = User.query.get(relationship.superadmin_id)
            if not superadmin:
                return jsonify({
                    'relationship': True,
                    'superadmin_name': 'Unknown'
                }), 200
                
            return jsonify({
                'relationship': True,
                'superadmin_name': superadmin.full_name
            }), 200
        else:
            return jsonify({
                'relationship': False,
                'message': 'No active relationship found'
            }), 200

    except Exception as e:
        print("[EXCEPTION]", str(e))
        return jsonify({'message': 'Internal Server Error'}), 500 