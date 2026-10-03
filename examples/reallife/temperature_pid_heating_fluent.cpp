/**
 * Real-life Example: Temperature PID Heating Control (Fluent Style)
 * 
 * Same as temperature_pid_heating.cpp but using the fluent API.
 * 
 * Hardware: ESP32/Arduino compatible
 * Library: Micro-Reactive
 */

#include <micro-reactive.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <random>
#include <cmath>
#include <memory>

using namespace rx;

// Simple logger observer for demo purposes
template<typename T>
class SimpleLogger : public IObserver<T> {
private:
    T last_value_;
    bool has_value_ = false;
    int count_ = 0;

public:
    void OnNext(const T& value) override {
        last_value_ = value;
        has_value_ = true;
        count_++;
    }
    void OnCompleted() override {}
    void OnError(const std::exception&) override {}
    T GetLastValue() const { return last_value_; }
    bool HasValue() const { return has_value_; }
    int GetCount() const { return count_; }
};  // <-- missing semicolon

// Simulated temperature sensor that produces noisy readings
class TemperatureSensor {
private:
    double current_temp_;
    double target_temp_;
    std::mt19937 rng_;
    std::normal_distribution<double> noise_;
    double ambient_temp_;
    double heating_power_;
    bool heating_active_;

public:
    TemperatureSensor(double initial_temp = 18.0, double ambient = 15.0)
        : current_temp_(initial_temp)
        , target_temp_(21.0)
        , rng_(std::random_device{}())
        , noise_(0.0, 0.3)  // ±0.3°C sensor noise
        , ambient_temp_(ambient)
        , heating_power_(2.5)  // °C per second when heating
        , heating_active_(false)
    {}

    void setTarget(double target) { target_temp_ = target; }
    void setHeating(bool active) { heating_active_ = active; }

    // Simulate one time step (dt seconds)
    double step(double dt) {
        // Natural cooling toward ambient (slower cooling)
        double cooling = (current_temp_ - ambient_temp_) * 0.02 * dt;
        current_temp_ -= cooling;

        // Heating effect
        if (heating_active_) {
            current_temp_ += heating_power_ * dt;
        }

        // Add sensor noise
        return current_temp_ + noise_(rng_);
    }

    double getCurrentTemp() const { return current_temp_; }
};

// Binary heating output observer
class HeatingController : public IObserver<double> {
private:
    double threshold_;
    bool heating_on_;
    std::function<void(bool)> on_change_;

public:
    HeatingController(double threshold = 0.5, std::function<void(bool)> callback = nullptr)
        : threshold_(threshold), heating_on_(false), on_change_(callback) {}

    void OnNext(const double& pid_output) override {
        bool new_state = pid_output > threshold_;
        if (new_state != heating_on_) {
            heating_on_ = new_state;
            if (on_change_) on_change_(heating_on_);
            std::cout << "[HEATING] " << (heating_on_ ? "ON" : "OFF")
                      << " (PID output: " << pid_output << ")" << std::endl;
        }
    }

    void OnCompleted() override {
        std::cout << "[HEATING] Controller completed" << std::endl;
    }

    void OnError(const std::exception& e) override {
        std::cerr << "[HEATING] Error: " << e.what() << std::endl;
    }

    bool isHeating() const { return heating_on_; }
};

int main() {
    std::cout << "=== Temperature PID Heating Control Demo (Fluent API) ===" << std::endl;
    std::cout << "Target: 21.0°C | PID: Kp=2.0, Ki=0.1, Kd=0.5 | dt=1.0s" << std::endl;
    std::cout << "Heating ON when PID output > 0.5" << std::endl;
    std::cout << "----------------------------------------" << std::endl;

    // Create simulated sensor
    TemperatureSensor sensor(18.0, 15.0);
    sensor.setTarget(21.0);

    // Create a subject to feed temperature readings
    auto temp_subject = std::make_shared<Subject<double>>();

    // PID parameters - tuned for the simulation
    const double setpoint = 21.0;
    const double kp = 2.0;
    const double ki = 0.1;
    const double kd = 0.5;
    const double dt = 1.0;
    const double min_output = 0.0;
    const double max_output = 1.0;

    // Create PID controller (FLUENT STYLE) - wrap subject in Observable first
    auto temp_observable = Observable<double>(temp_subject);
    auto pid_output = temp_observable.PID(setpoint, kp, ki, kd, dt, min_output, max_output);

    // Create heating controller (binary output)
    HeatingController heating_ctrl(0.5, [&](bool on) {
        sensor.setHeating(on);
    });

    // Subscribe heating controller to PID output (fluent, store subscription)
    auto heating_sub = pid_output.Get()->Subscribe(std::make_shared<HeatingController>(heating_ctrl));

    // Also log temperature and PID output for monitoring (store subscriptions)
    auto temp_logger = std::make_shared<SimpleLogger<double>>();
    auto pid_logger = std::make_shared<SimpleLogger<double>>();

    auto temp_sub = temp_subject->Subscribe(temp_logger);
    auto pid_sub = pid_output.Get()->Subscribe(pid_logger);

    // Simulation loop
    const int steps = 60;  // 60 seconds
    for (int i = 0; i < steps; ++i) {
        double reading = sensor.step(dt);
        temp_subject->OnNext(reading);

        // Print status every 5 seconds
        if (i % 5 == 0) {
            std::cout << "t=" << i << "s | Temp: " << reading
                      << "°C | PID: " << pid_logger->GetLastValue()
                      << " | Heat: " << (heating_ctrl.isHeating() ? "ON" : "OFF") << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Speed up demo
    }

    temp_subject->OnCompleted();

    std::cout << "----------------------------------------" << std::endl;
    std::cout << "Simulation complete." << std::endl;
    std::cout << "Final temperature: " << sensor.getCurrentTemp() << "°C" << std::endl;

    return 0;
}