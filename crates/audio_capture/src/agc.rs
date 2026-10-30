pub struct Agc {
    _sample_rate: f32,
    channels: usize,

    target_level: f32,
    max_gain: f32,
    noise_floor: f32,

    current_gain: f32,
    envelope: f32,

    attack_coeff: f32,
    release_coeff: f32,
}

impl Agc {
    pub fn new(sample_rate: u32, channels: usize) -> Self {
        let sample_rate = sample_rate as f32;

        let attack_coeff = (-1.0 / (0.005 * sample_rate)).exp();
        let release_coeff = (-1.0 / (0.5 * sample_rate)).exp();

        Self {
            _sample_rate: sample_rate as f32,
            channels,
            target_level: 0.05,
            max_gain: 6.0,
            noise_floor: 0.05,
            current_gain: 1.0,
            envelope: 0.0,
            attack_coeff,
            release_coeff,
        }
    }

    pub fn process(&mut self, data: &mut [f32]) {
        for frame in data.chunks_mut(self.channels) {
            let peak = frame.iter().fold(0.0f32, |a, &b| a.max(b.abs()));

            if peak > self.envelope {
                self.envelope =
                    self.attack_coeff * self.envelope + (1.0 - self.attack_coeff) * peak;
            } else {
                self.envelope =
                    self.release_coeff * self.envelope + (1.0 - self.release_coeff) * peak;
            }

            let mut gain = 1.0;

            if self.envelope > self.noise_floor {
                let desired_gain = self.target_level / self.envelope.max(0.0000001);
                gain = desired_gain.clamp(0.05, self.max_gain);
            }

            // tune more aggressively down than up
            if gain < self.current_gain {
                self.current_gain = self.current_gain * 0.99 + gain * 0.01;
            } else {
                self.current_gain = self.current_gain * 0.9995 + gain * 0.0005;
            }

            for sample in frame.iter_mut() {
                *sample *= self.current_gain;

                *sample = *sample / (1.0 + sample.abs())
            }
        }
    }
}
