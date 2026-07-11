use robotdreams_core::{
    CoordinateDebugOverlayOptions, ObservationRequest, RobotDreams, RobotDreamsSnapshot,
    scene_graph::prepare_observation_scene,
};
use robotdreams_renderer::{
    FrameBuffer, FrameKind, NativeRenderer, RenderOutput, SceneGraphSample,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordingArtifact {
    StateTrace,
    Frames,
    VideoFrames,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordingRequest {
    pub observation: ObservationRequest,
    pub fps: f32,
    pub seconds: f32,
    pub artifacts: Vec<RecordingArtifact>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordedFrame {
    pub index: usize,
    pub timestamp_sec: f64,
    pub output: RenderOutput,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VideoFrameStream {
    pub kind: FrameKind,
    pub fps: f32,
    pub frames: Vec<FrameBuffer>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordingOutput {
    pub frame_count: usize,
    pub duration_sec: f32,
    pub frames: Vec<RecordedFrame>,
    pub state_trace: Vec<RobotDreamsSnapshot>,
    pub video_stream: Option<VideoFrameStream>,
}

#[derive(Clone, Debug, Default)]
pub struct NativeRecorder {
    renderer: NativeRenderer,
}

impl NativeRecorder {
    pub fn new() -> Self {
        Self {
            renderer: NativeRenderer::new(),
        }
    }

    pub fn record(
        &self,
        dreams: &mut RobotDreams,
        request: &RecordingRequest,
    ) -> Result<RecordingOutput, String> {
        self.record_with_coordinate_debug_overlay(
            dreams,
            request,
            CoordinateDebugOverlayOptions::default(),
        )
    }

    pub fn record_with_coordinate_debug_overlay(
        &self,
        dreams: &mut RobotDreams,
        request: &RecordingRequest,
        coordinate_debug_overlay: CoordinateDebugOverlayOptions,
    ) -> Result<RecordingOutput, String> {
        let fps = request.fps;
        if fps <= 0.0 {
            return Err("recording fps must be positive".to_string());
        }
        if request.seconds < 0.0 {
            return Err("recording seconds must be zero or positive".to_string());
        }

        let dt = 1.0 / fps;
        let frame_count = (request.seconds * fps).ceil() as usize + 1;
        let want_frames = request.artifacts.contains(&RecordingArtifact::Frames);
        let want_trace = request.artifacts.contains(&RecordingArtifact::StateTrace);
        let want_video = request.artifacts.contains(&RecordingArtifact::VideoFrames);
        let mut frames = Vec::new();
        let mut state_trace = Vec::new();
        let mut video_frames = Vec::new();
        let mut previous_render_sample: Option<SceneGraphSample> = None;

        for index in 0..frame_count {
            if index > 0 {
                dreams.advance_seconds(dt);
            }

            let snapshot = dreams.snapshot();
            if want_trace {
                state_trace.push(snapshot.clone());
            }

            if want_frames || want_video {
                let scene = prepare_observation_scene(
                    dreams.scene_graph_with_coordinate_debug_overlay(coordinate_debug_overlay),
                    &request.observation,
                );
                let current_sample = SceneGraphSample {
                    scene,
                    state: Some(snapshot.clone()),
                };
                let use_state_samples = request
                    .observation
                    .shutter_policy
                    .map(|policy| policy.exposure_sec > 0.0 && policy.samples > 1)
                    .unwrap_or(false)
                    && previous_render_sample.is_some();
                let output = if use_state_samples {
                    self.renderer.render_scene_samples(
                        &[
                            previous_render_sample
                                .clone()
                                .expect("checked previous sample"),
                            current_sample.clone(),
                        ],
                        &request.observation,
                    )?
                } else {
                    self.renderer.render(
                        &current_sample.scene,
                        current_sample.state.clone(),
                        &request.observation,
                    )?
                };
                previous_render_sample = Some(current_sample);
                if want_video
                    && let Some(frame) = output
                        .frames
                        .iter()
                        .find(|frame| frame.kind == FrameKind::DebugRgb)
                {
                    video_frames.push(frame.clone());
                }
                if want_frames {
                    frames.push(RecordedFrame {
                        index,
                        timestamp_sec: snapshot.clock_sec,
                        output,
                    });
                }
            }
        }

        let video_stream = if want_video {
            Some(VideoFrameStream {
                kind: FrameKind::DebugRgb,
                fps,
                frames: video_frames,
            })
        } else {
            None
        };

        Ok(RecordingOutput {
            frame_count,
            duration_sec: request.seconds,
            frames,
            state_trace,
            video_stream,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use robotdreams_core::{ObservationRequest, ObservationView, scene_graph::AUTO_CAMERA_ID};

    use super::{NativeRecorder, RecordingArtifact, RecordingRequest};

    fn project_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("recorder crate has workspace parent")
            .to_path_buf()
    }

    #[test]
    fn records_state_trace_without_visual_renderer() {
        let mut dreams = robotdreams_core::RobotDreams::open(
            project_root().join("examples/puppyarm/project.json"),
        )
        .expect("open project");

        let output = NativeRecorder::new()
            .record(
                &mut dreams,
                &RecordingRequest {
                    observation: ObservationRequest {
                        camera_id: None,
                        views: vec![ObservationView::State],
                        resolution: [4, 4],
                        segmentation_policy: None,
                        shutter_policy: None,
                        render_settings: None,
                    },
                    fps: 5.0,
                    seconds: 0.4,
                    artifacts: vec![RecordingArtifact::StateTrace],
                },
            )
            .expect("record state trace");

        assert_eq!(output.frame_count, 3);
        assert_eq!(output.state_trace.len(), 3);
        assert!(output.frames.is_empty());
        assert!(output.state_trace[2].clock_sec > output.state_trace[0].clock_sec);
    }

    #[test]
    fn records_video_frame_stream_from_camera() {
        let mut dreams = robotdreams_core::RobotDreams::open(
            project_root().join("examples/puppyarm/project.json"),
        )
        .expect("open project");

        let output = NativeRecorder::new()
            .record(
                &mut dreams,
                &RecordingRequest {
                    observation: ObservationRequest {
                        camera_id: Some("overhead_camera".to_string()),
                        views: vec![ObservationView::DebugRgb],
                        resolution: [8, 6],
                        segmentation_policy: None,
                        shutter_policy: None,
                        render_settings: None,
                    },
                    fps: 2.0,
                    seconds: 0.5,
                    artifacts: vec![RecordingArtifact::VideoFrames],
                },
            )
            .expect("record video frames");
        let stream = output.video_stream.expect("video stream");

        assert_eq!(stream.frames.len(), 2);
        assert!(stream.frames.iter().all(|frame| frame.width == 8));
        assert!(
            stream
                .frames
                .iter()
                .all(|frame| frame.bytes.starts_with(b"\x89PNG\r\n\x1a\n"))
        );
    }

    #[test]
    fn records_video_frame_stream_from_auto_camera() {
        let mut dreams = robotdreams_core::RobotDreams::open(
            project_root().join("examples/puppyarm/project.json"),
        )
        .expect("open project");

        let output = NativeRecorder::new()
            .record(
                &mut dreams,
                &RecordingRequest {
                    observation: ObservationRequest {
                        camera_id: Some(AUTO_CAMERA_ID.to_string()),
                        views: vec![ObservationView::DebugRgb],
                        resolution: [8, 6],
                        segmentation_policy: None,
                        shutter_policy: None,
                        render_settings: None,
                    },
                    fps: 2.0,
                    seconds: 0.0,
                    artifacts: vec![RecordingArtifact::VideoFrames],
                },
            )
            .expect("record auto-camera video frame");
        let stream = output.video_stream.expect("video stream");

        assert_eq!(stream.frames.len(), 1);
        assert!(stream.frames[0].bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
    }
}
