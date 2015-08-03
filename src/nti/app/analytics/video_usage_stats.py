#!/usr/bin/env python
# -*- coding: utf-8 -*-

from heapq import nlargest
from collections import namedtuple

from nti.ntiids.ntiids import find_object_with_ntiid
from nti.dataserver.interfaces import IEnumerableEntityContainer


_VideoInfo = namedtuple( '_VideoInfoData',
                            ('title', 
                            'ntiid', 
                            'session_count', 
                            'view_event_count', 
                            'watch_times', 
                            'video_duration', 
                            'percentage_watched_completely', 
                            'falloff_rate'))
_VideoFalloffRate = namedtuple( '_VideoFalloffRate', 
                            ('drop25count', 'drop25percentage',
                            'drop50count', 'drop50percentage',
                            'drop75count', 'drop75percentage',
                            'drop100count', 'drop100percentage'))
_VideoAverageWatchTimes = namedtuple('_VideoAverageWatchTimes', 
                                    ('average_total_watch_time', 'average_session_watch_time'))
_GeneralVideoInfo = namedtuple('_GeneralVideoInfo', ('percentage_completed_video_sessions'))

class VideoUsageReport( object ):

    view_counts = {}

    def instructor_usernames(self):
        return {x.id.lower() for x in self.course.instructors}

    def _get_counts_for_videos(self):
        
        view_counts = self.view_counts
        video_events = self.video_events
        
        all_sessions = {}
        self.all_sessions = all_sessions
        
        # If we've already gotten view counts, don't do it again.
        if len( view_counts ) != 0:
            return

        for event in self.video_events:
            
            # Create a list of videos.
            # Check if we already have an entry for this video
            if event.ResourceId not in view_counts:
                # If this is a new video, prepare to store
                # session_ids and video events for it,
                view_counts[event.ResourceId] = { 'session_ids': [], 'video_events': [], 'session_end_times': [] }
                
            # If we haven't seen this session id before, add it to the list
            if event.SessionID not in view_counts[event.ResourceId]['session_ids']:
                view_counts[event.ResourceId]['session_ids'].append( event.SessionID )
            # Add this video event to the list associated with this video
            view_counts[event.ResourceId]['video_events'].append( event )
            
            # Create a list of sessions
            # Check if we already have an entry for this session
            if event.SessionID not in all_sessions:
                # We haven't seen this session yet, so create a spot for it.
                all_sessions[event.SessionID] = {}
            # Check if we have an entry for this video for its session
            if event.ResourceId not in all_sessions[event.SessionID]:
                # We haven't seen this video in this session yet 
                all_sessions[event.SessionID][event.ResourceId] = {'total_duration_watched': 0,
                                                                'watched_90_percent': False, 
                                                                'watched_ending': False, 
                                                                'max_session_end_time': 0 }
            # Go to the appropriate video and increment the counter and set conditions
            all_sessions[event.SessionID][event.ResourceId]['total_duration_watched'] += event.Duration
            if all_sessions[event.SessionID][event.ResourceId]['total_duration_watched'] > event.MaxDuration*0.9: #TODO: change this to a constant
                all_sessions[event.SessionID][event.ResourceId]['watched_90_percent'] = True
            
            # Determine how far this session went into the video
            if event.VideoEndTime > all_sessions[event.SessionID][event.ResourceId]['max_session_end_time']:
                all_sessions[event.SessionID][event.ResourceId]['max_session_end_time'] = event.VideoEndTime
                
                # If we increase the max end time, check if it's in the last 10% of the video
                if event.VideoEndTime >= event.MaxDuration*0.9:
                    all_sessions[event.SessionID][event.ResourceId]['watched_ending'] = True
            
        return

    def _build_video_info(self, options):
        
        def vids_with_most_sessions( d,n ):
            # Note: operator.attrgetter may be faster than a lambda function
            return nlargest( n, d, key = lambda vid: vid.session_count )
        
        self._get_counts_for_videos()
        
        video_data = []
        counter = 0
        for ntiid in self.view_counts:
            # Get the average watch times for each video
            view_events = self.view_counts[ntiid]['video_events']
            total_time = 0
            users = {}
#             number_users_watched_completely = 0
            for event in view_events:
                total_time += event.Duration
                # make a list of distinct users
                # TODO: This would also make more sense with a dictionary instead of a list
                if event.user not in users:
                    users[event.user] = [0, False, False]
                users[event.user][0] += event.Duration
                # TODO: Using a 90% threshold for the moment. Will want to change this to a constant later on.
                if users[event.user][0] > event.MaxDuration * 0.9:
                    users[event.user][1] = True # If the total watch time exceeds 90% of the video
                if event.VideoEndTime >= event.MaxDuration * 0.9:
                    users[event.user][2] = True # If at least one event ends in the last 10% of the video

            # Total time a video was watched / number of users enrolled = average watch time per user
            if self.course.count_all_students > 0:
                average_total_watch_time = total_time / self.course.count_all_students
            else:
                average_total_watch_time = 0 # only would happen if 0 students were enrolled in the course
                
            # Total time a video was watched / number of sessions = average watch time per session
            average_session_watch_time = total_time / float( len( self.view_counts[ntiid]['session_ids'] ) )
            
            watch_data = _VideoAverageWatchTimes('%s:%02d' % divmod( int( average_total_watch_time ), 60 ),
                                                 '%s:%02d' % divmod( int( average_session_watch_time ), 60 ) )
            
            number_users_watched_completely = len( [x for x in users.values() if x[1] and x[2]] )
            
            if self.course.count_all_students > 0:
                percentage_users_watched_completely = number_users_watched_completely / self.course.count_all_students
            else:
                percentage_users_watched_completely = 0
                
            str_percentage_users_watched_completely = '%d%%' % int( percentage_users_watched_completely*100 )
            
            falloff_data = self._build_video_falloff_data( ntiid )
            
            video_duration = view_events[0].MaxDuration
            str_video_duration = '%s:%02d' % divmod( int( video_duration ), 60 )
            
            # Export the data for this row
            data = _VideoInfo( find_object_with_ntiid( ntiid ).title, 
                                ntiid, 
                                len( self.view_counts[ntiid]['session_ids'] ), 
                                len( self.view_counts[ntiid]['video_events'] ),
                                watch_data, 
                                str_video_duration,
                                str_percentage_users_watched_completely,
                                falloff_data )
            video_data.append( data )
            counter+=1
            
        video_data.sort( key=lambda vid: vid.title )
        
        options['all_videos'] = video_data
        
        # Get the top videos 
        #TODO: This number should probably be a constant or a parameter
        top_video_data = vids_with_most_sessions(video_data, 6) 
        
        options['top_videos'] = top_video_data
        
    def _build_general_video_stats(self, options):
        
        # Get percentage of videos watched all the way through
        all_sessions = self.all_sessions
        
        # Get the number of completed video sessions for each video
        number_of_completed_video_sessions = 0
        number_of_video_sessions = 0
        for session in all_sessions.values():
            for video_id in session.keys():
                if session[video_id]['watched_90_percent'] and session[video_id]['watched_ending']:
                    number_of_completed_video_sessions += 1
                number_of_video_sessions += 1
                
                # add the ending time for this video session
                self.view_counts[video_id]['session_end_times'].append(session[video_id]['max_session_end_time'])


    def _build_video_falloff_data(self, ntiid):

        session_count = len(self.view_counts[ntiid]['session_end_times'])
        # Get the duration of the video from the first event (TODO: better way to do this?)
        video_duration = self.view_counts[ntiid]['video_events'][0].MaxDuration
            
        drop25count = drop50count = drop75count = drop100count = 0
        for end_time in self.view_counts[ntiid]['session_end_times']:
            if end_time <= video_duration*0.25:
                drop25count += 1
            elif end_time <= video_duration*0.5:
                drop50count += 1
            elif end_time <= video_duration*0.75:
                drop75count += 1
            else:
                drop100count += 1
                    
        drop25percentage = round(drop25count/float(session_count)*100)
        drop50percentage = round(drop50count/float(session_count)*100)
        drop75percentage = round(drop75count/float(session_count)*100)
        drop100percentage = round(drop100count/float(session_count)*100)

        falloff_data = _VideoFalloffRate(drop25count, drop25percentage,
                                         drop50count, drop50percentage,
                                         drop75count, drop75percentage,
                                         drop100count, drop100percentage)
            
        return falloff_data
    
    
    def get_video_usage_stats(self, video_events, course):
        
        self.course = course

        results = {}
        # Determine the number of students enrolled in this course
        public_scope = course.SharingScopes.get( 'Public', None )
        purchased_scope = course.SharingScopes.get( 'Purchased', None )
        non_public_users = set()
        for scope_name in course.SharingScopes:
            scope = course.SharingScopes.get( scope_name, None )
    
            if scope is not None \
                and scope not in (public_scope, purchased_scope):
    
                # If our scope is not 'public'-ish, store it separately.
                # All credit-type users should end up in ForCredit.
                scope_users = {x.lower() for x in IEnumerableEntityContainer(scope).iter_usernames()}
                scope_users = scope_users - self.instructor_usernames()
                results[scope_name] = scope_users
                non_public_users = non_public_users.union( scope_users )
    
        all_users = {x.lower() for x in IEnumerableEntityContainer(public_scope).iter_usernames()}
        
        students = all_users - self.instructor_usernames()
        
        course.count_all_students = len(students)

        options = {}
        self.video_events = video_events
        self._get_counts_for_videos()
        self._build_general_video_stats( options )
        self._build_video_info( options )
        self.options = options

        return options
